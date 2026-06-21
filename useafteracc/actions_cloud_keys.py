#!/usr/bin/env python3
# actions_cloud_keys.py — AWS IAM 액세스키 자동 로테이션/감사/은퇴(1개만 유지)
import os, sys, json, argparse, subprocess, time
from datetime import datetime, timezone, timedelta

import boto3
import botocore
from botocore.config import Config

def jlog(level, msg, **kw):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "level": level,
        "msg": msg,
    }
    payload.update(kw)
    print(json.dumps(payload, ensure_ascii=False))

def mask_id(i):
    if not i: return ""
    return i[:4] + "*" * max(0, len(i) - 8) + i[-4:]

def _persist_json(fname, obj):
    outdir = os.environ.get("OUTPUT_DIR", "")
    if not outdir:
        return None
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, fname)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
    os.chmod(path, 0o600)
    return path

_AWS_CFG = Config(
    connect_timeout=4,
    read_timeout=6,
    retries={"max_attempts": 3, "mode": "standard"},
)

def _session(profile=None, region=None, access_key=None, secret_key=None, session_token=None):
    if access_key and secret_key:
        return boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region,
        )
    return boto3.Session(profile_name=profile, region_name=region)

def _client(service, profile=None, region=None, **kw):
    return _session(profile, region, **kw).client(service, config=_AWS_CFG)

def _iam(profile=None, region=None, **kw):
    return _client("iam", profile, region, **kw)

def _sts(profile=None, region=None, **kw):
    return _client("sts", profile, region, **kw)

def _cloudtrail(profile=None, region=None, **kw):
    return _client("cloudtrail", profile, region or "us-east-1", **kw)

def list_access_keys(user, profile, region):
    iam = _iam(profile, region)
    resp = iam.list_access_keys(UserName=user)
    return resp.get("AccessKeyMetadata", [])

def create_access_key(user, profile, region):
    iam = _iam(profile, region)
    return iam.create_access_key(UserName=user)["AccessKey"]

def update_access_key_status(user, access_key_id, status, profile, region):
    iam = _iam(profile, region)
    iam.update_access_key(UserName=user, AccessKeyId=access_key_id, Status=status)

def delete_access_key(user, access_key_id, profile, region):
    iam = _iam(profile, region)
    iam.delete_access_key(UserName=user, AccessKeyId=access_key_id)

def sts_whoami_profile(profile, region):
    return _sts(profile, region).get_caller_identity()

def sts_verify_direct(access_key, secret_key, region):
    regions = ["us-east-1"]
    if region and region not in regions:
        regions.append(region)
    last_err = None
    for rg in regions:
        try:
            c = _sts(region=rg, access_key=access_key, secret_key=secret_key)
            return c.get_caller_identity()
        except botocore.exceptions.ClientError as e:
            last_err = e
        except botocore.exceptions.EndpointConnectionError as e:
            last_err = e
    raise last_err

def cloudtrail_lookup_by_access_key(access_key_id, hours, profile, region):
    ct = _cloudtrail(profile, region)
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=int(hours or 720))
    events, token = [], None
    while True:
        kwargs = {
            "LookupAttributes": [{"AttributeKey": "AccessKeyId", "AttributeValue": access_key_id}],
            "StartTime": start,
            "EndTime": end,
            "MaxResults": 50,
        }
        if token:
            kwargs["NextToken"] = token
        resp = ct.lookup_events(**kwargs)
        events.extend(resp.get("Events", []))
        token = resp.get("NextToken")
        if not token:
            break
    return events

def write_profile(profile_name, access_key, secret_key, region):
    base_env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    cmds = [
        ["aws", "configure", "set", "aws_access_key_id", access_key, "--profile", profile_name],
        ["aws", "configure", "set", "aws_secret_access_key", secret_key, "--profile", profile_name],
        ["aws", "configure", "set", "region", region or "ap-northeast-2", "--profile", profile_name],
        ["aws", "configure", "set", "output", "json", "--profile", profile_name],
    ]
    for cmd in cmds:
        subprocess.run(cmd, check=True, env=base_env)
    return True

def _auto_pick_profile(region):
    envp = os.environ.get("AWS_PROFILE")
    if envp:
        try:
            who = sts_whoami_profile(envp, region)
            jlog("INFO", "auto profile: picked", profile=envp, arn=who.get("Arn"))
            return envp
        except Exception:
            pass
    try:
        from configparser import ConfigParser
        paths = [os.path.expanduser("~/.aws/credentials"), os.path.expanduser("~/.aws/config")]
        names = []
        for p in paths:
            if not os.path.exists(p): continue
            cp = ConfigParser()
            cp.read(p)
            for sect in cp.sections():
                n = sect.replace("profile ", "")
                if n not in names:
                    names.append(n)
        for n in names:
            try:
                who = sts_whoami_profile(n, region)
                jlog("INFO", "auto profile: picked", profile=n, arn=who.get("Arn"))
                return n
            except Exception:
                continue
    except Exception:
        pass
    jlog("ERROR", "auto profile: no working credentials found")
    return None

def _load_secret_for(access_key_id):
    outdir = os.environ.get("OUTPUT_DIR", "")
    if not outdir or not os.path.isdir(outdir):
        return None
    try:
        for fn in sorted(os.listdir(outdir), reverse=True):
            if fn.endswith(".json") and access_key_id in fn:
                with open(os.path.join(outdir, fn), "r", encoding="utf-8") as f:
                    d = json.load(f)
                    if d.get("AccessKeyId") == access_key_id:
                        return d.get("SecretAccessKey")
    except Exception:
        return None
    return None

def orchestrate_keep_one(user, new_profile, make_default, retire, hours, admin_profile, region, dry=False):
    if not admin_profile:
        admin_profile = _auto_pick_profile(region)
    if not admin_profile:
        try:
            sts_whoami_profile(None, region)
            admin_profile = None
            jlog("INFO", "admin profile OK", profile=None, arn="env-creds")
        except Exception as e:
            jlog("ERROR", "admin profile invalid", profile=None, error=str(e))
            return

    try:
        who = sts_whoami_profile(admin_profile, region)
        jlog("INFO", "admin profile OK", profile=admin_profile, arn=who.get("Arn"))
    except Exception as e:
        jlog("ERROR", "admin profile invalid", profile=admin_profile, error=str(e))
        return

    keys = list_access_keys(user, admin_profile, region)
    keys_sorted = sorted(keys, key=lambda x: x["CreateDate"])
    new_key_obj = None
    old_key_id = None
    keep_key_id = None

    if len(keys_sorted) >= 2:
        newest = keys_sorted[-1]["AccessKeyId"]
        oldest = keys_sorted[0]["AccessKeyId"]
        keep_key_id, old_key_id = newest, oldest
        jlog("INFO", "already two keys; skip creation", keep=keep_key_id, retire=old_key_id)

        secret = _load_secret_for(keep_key_id)
        if secret:
            try:
                write_profile(new_profile, keep_key_id, secret, region)
                jlog("INFO", "profile written from secret store", profile=new_profile, keep=keep_key_id)
            except Exception as e:
                jlog("ERROR", "profile write failed (existing key)", error=str(e))
        else:
            jlog("INFO", "profile write skipped (secret unknown for existing key)", profile=new_profile, keep=keep_key_id)
    else:
        if dry:
            jlog("INFO", "[DRY_RUN] would create new key for user", user=user)
            return
        try:
            created = create_access_key(user, admin_profile, region)
            new_key_obj = {
                "UserName": created["UserName"],
                "AccessKeyId": created["AccessKeyId"],
                "SecretAccessKey": created["SecretAccessKey"],
                "CreateDate": created["CreateDate"],
            }
            path = _persist_json(f"aws_{user}_{created['AccessKeyId']}.json", {
                "UserName": created["UserName"],
                "AccessKeyId": created["AccessKeyId"],
                "SecretAccessKey": created["SecretAccessKey"],
                "CreateDate": created["CreateDate"].isoformat(),
            })
            jlog("INFO", "[STEP] created new key", id=mask_id(created["AccessKeyId"]), stored_at=path or "(OUTPUT_DIR not set; not persisted)")
        except botocore.exceptions.ClientError as e:
            jlog("ERROR", "create key failed", error=str(e))
            return

        if keys_sorted:
            old_key_id = keys_sorted[0]["AccessKeyId"]

        ok = False
        for attempt in range(12):  # ~60s
            try:
                who2 = sts_verify_direct(new_key_obj["AccessKeyId"], new_key_obj["SecretAccessKey"], region)
                jlog("INFO", "[STEP] STS direct verify OK", arn=who2.get("Arn"))
                ok = True
                break
            except Exception as e:
                jlog("ERROR", "[STEP] STS direct verify failed; retry", attempt=attempt+1, error=str(e))
                time.sleep(5)
        if not ok:
            jlog("ERROR", "STS verify failed")
            return

        try:
            write_profile(new_profile, new_key_obj["AccessKeyId"], new_key_obj["SecretAccessKey"], region)
            jlog("INFO", "[STEP] wrote profile", profile=new_profile)
            who3 = sts_whoami_profile(new_profile, region)
            jlog("INFO", "[STEP] STS verify via profile OK", arn=who3.get("Arn"))
            keep_key_id = new_key_obj["AccessKeyId"]
        except Exception as e:
            jlog("ERROR", "[STEP] profile write/verify failed", error=str(e))
            return

    if old_key_id:
        try:
            events = cloudtrail_lookup_by_access_key(old_key_id, hours, admin_profile, region)
            path = _persist_json(f"cloudtrail_{user}_{old_key_id}_{int(time.time())}.json", {"AccessKeyId": old_key_id, "events": events})
            jlog("INFO", "[STEP] CloudTrail audit done", old=mask_id(old_key_id), events=len(events), stored_at=path or "(not saved)")

            if retire in ("delete", "inactive"):
                if not dry:
                    update_access_key_status(user, old_key_id, "Inactive", admin_profile, region)
                msg = f"inactivated old={mask_id(old_key_id)}"
                if retire == "delete":
                    if not dry:
                        delete_access_key(user, old_key_id, admin_profile, region)
                    msg += " | deleted"
                jlog("INFO", "[STEP] retired old key", result=msg)
        except botocore.exceptions.ClientError as e:
            jlog("ERROR", "retire/audit failed", error=str(e))
    else:
        jlog("INFO", "no old key to retire (fresh user or already single-key)")

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="action", required=True)

    orch = sub.add_parser("orchestrate_keep_one", help="새 키 생성/검증/프로필기록 + 이전 키 감사/은퇴(1개만 유지)")
    orch.add_argument("--user", required=True)
    orch.add_argument("--new-profile", required=True)
    orch.add_argument("--make-default", type=int, default=0)
    orch.add_argument("--retire", choices=["delete", "inactive"], default="delete")
    orch.add_argument("--hours", type=int, default=720)

    ap.add_argument("--aws-profile", default=None, help="관리 작업에 사용할 관리자 프로필(생략 시 자동 탐색)")
    ap.add_argument("--region", default="ap-northeast-2")

    args = ap.parse_args()
    jlog("INFO", "exec start", action=args.action, provider="aws", aws_profile=args.aws_profile, region=args.region, user=getattr(args, "user", None), new_profile=getattr(args, "new_profile", None), make_default=getattr(args, "make_default", None), retire=getattr(args, "retire", None), hours=getattr(args, "hours", None))

    if args.action == "orchestrate_keep_one":
        dry = os.getenv("DRY_RUN", "0") == "1"
        orchestrate_keep_one(
            user=args.user,
            new_profile=args.new_profile,
            make_default=bool(args.make_default),
            retire=args.retire,
            hours=args.hours,
            admin_profile=args.aws_profile,
            region=args.region,
            dry=dry,
        )

if __name__ == "__main__":
    main()

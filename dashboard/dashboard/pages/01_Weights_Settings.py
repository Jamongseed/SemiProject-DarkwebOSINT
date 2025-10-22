# pages/01_Weights_Settings.py
import streamlit as st
import pandas as pd
import altair as alt
import json

from lib.weights_presets import ORDER, DEFAULT_WEIGHTS, PRESETS, normalize

st.set_page_config(page_title="가중치 설정", layout="wide")
st.title("가중치 설정")

# 세션에 커스텀 프리셋 저장소 준비
if "custom_presets" not in st.session_state:
    st.session_state["custom_presets"] = {}  # name -> raw dict

# 1) 프리셋 선택/적용 영역
st.subheader("프리셋")
builtin_names = [key for key in PRESETS.keys()]
custom_names  = list(st.session_state["custom_presets"].keys())

tab_builtin, tab_custom = st.tabs(["내장 프리셋", "내 프리셋"])

with tab_builtin:
    key = st.selectbox(
        "내장 프리셋을 선택해 주세요.",
        options=builtin_names,
        format_func=lambda k: PRESETS[k]["label"]
    )
    sel = PRESETS[key]
    st.markdown(f"**개요**: {sel['notes']}")
    st.markdown("**추천 상황**:")
    for w in sel["when"]:
        st.markdown(f"- {w}")

    # 미리보기 도넛
    nw = normalize(sel["raw"])
    w_df = pd.DataFrame({"component": list(nw.keys()), "weight": list(nw.values())})
    pie = alt.Chart(w_df).mark_arc(innerRadius=60).encode(
        theta="weight:Q", color="component:N", tooltip=["component","weight"]
    ).properties(height=260)
    st.altair_chart(pie, use_container_width=True)

    if st.button("이 프리셋 적용"):
        st.session_state["weights_raw"]  = sel["raw"].copy()
        st.session_state["weights_norm"] = normalize(sel["raw"])
        st.success("프리셋을 적용했습니다. 아래 슬라이더에서 미세 조정하실 수 있습니다.")

with tab_custom:
    if custom_names:
        key2 = st.selectbox("저장된 내 프리셋을 선택해 주세요.", options=custom_names)
        raw2 = st.session_state["custom_presets"][key2]
        nw2 = normalize(raw2)
        w_df2 = pd.DataFrame({"component": list(nw2.keys()), "weight": list(nw2.values())})
        pie2 = alt.Chart(w_df2).mark_arc(innerRadius=60).encode(
            theta="weight:Q", color="component:N", tooltip=["component","weight"]
        ).properties(height=260)
        st.altair_chart(pie2, use_container_width=True)
        c1, c2 = st.columns(2)
        with c1:
            if st.button("이 프리셋 적용", key="apply_custom"):
                st.session_state["weights_raw"]  = raw2.copy()
                st.session_state["weights_norm"] = normalize(raw2)
                st.success("프리셋을 적용했습니다.")
        with c2:
            if st.button("이 프리셋 삭제", key="del_custom"):
                del st.session_state["custom_presets"][key2]
                st.success("삭제했습니다.")
                st.rerun()
    else:
        st.info("아직 저장된 내 프리셋이 없습니다.")

# 2) 슬라이더 미세 조정
st.subheader("슬라이더 미세 조정")
st.caption("슬라이더의 합계를 자유롭게 조정하실 수 있습니다. 내부적으로 합계 100 기준으로 정규화합니다.")
weights_raw = st.session_state.get("weights_raw", DEFAULT_WEIGHTS.copy())
cols = st.columns(4)
new_raw = {}
for i, k in enumerate(ORDER):
    with cols[i % 4]:
        new_raw[k] = st.slider(k, 0, 50, int(weights_raw.get(k, DEFAULT_WEIGHTS[k])), 1)

total = sum(new_raw.values())
if total == 0:
    st.error("가중치 합계가 0입니다. 하나 이상 값을 올려 주십시오.")
else:
    st.success(f"현재 합계: {total} → 내부적으로 100 기준으로 정규화됩니다.")

st.session_state["weights_raw"]  = new_raw
st.session_state["weights_norm"] = normalize(new_raw)

# 도넛 미리보기
nw = st.session_state["weights_norm"]
w_df = pd.DataFrame({"component": list(nw.keys()), "weight": list(nw.values())})
pie = alt.Chart(w_df).mark_arc(innerRadius=60).encode(
    theta="weight:Q", color="component:N", tooltip=["component","weight"]
).properties(height=300)
st.altair_chart(pie, use_container_width=True)

# 3) 프리셋 저장/불러오기
st.subheader("프리셋 저장/불러오기")
c1, c2, c3, c4 = st.columns(4)
with c1:
    name = st.text_input("내 프리셋 이름", placeholder="예: 랜섬웨어_우선")
with c2:
    if st.button("이 이름으로 저장"):
        if not name.strip():
            st.error("프리셋 이름을 입력해 주십시오.")
        else:
            st.session_state["custom_presets"][name.strip()] = st.session_state["weights_raw"].copy()
            st.success("저장했습니다.")
with c3:
    st.download_button(
        "현재 가중치 JSON 내보내기",
        data=json.dumps(st.session_state["weights_raw"], ensure_ascii=False, indent=2).encode("utf-8"),
        file_name="weights.json",
        mime="application/json",
    )
with c4:
    up = st.file_uploader("JSON 불러오기", type=["json"], label_visibility="collapsed")
    if up:
        try:
            loaded = json.loads(up.read().decode("utf-8"))
            merged = {k: float(loaded.get(k, DEFAULT_WEIGHTS[k])) for k in ORDER}
            st.session_state["weights_raw"]  = merged
            st.session_state["weights_norm"] = normalize(merged)
            st.success("불러왔습니다.")
            st.rerun()
        except Exception as e:
            st.error(f"불러오기 실패: {e}")


# pages/02_Weights_Guide.py
import streamlit as st
import pandas as pd
import altair as alt
from lib.weights_presets import PRESETS, normalize

st.set_page_config(page_title="가중치 유스케이스 가이드", layout="wide")
st.title("가중치 유스케이스 가이드")

st.caption("각 프리셋은 서로 다른 위험 신호를 강조하도록 설계되었습니다. 상황에 맞게 선택하여 적용하시고, 필요하시면 ‘가중치 설정’ 페이지에서 슬라이더로 미세 조정하실 수 있습니다.")

for key, meta in PRESETS.items():
    with st.container(border=True):
        left, right = st.columns([2,1])
        with left:
            st.subheader(meta["label"])
            st.markdown(f"**개요**: {meta['notes']}")
            st.markdown("**추천 상황**:")
            for w in meta["when"]:
                st.markdown(f"- {w}")
            if st.button(f"이 프리셋 적용", key=f"apply_{key}"):
                st.session_state["weights_raw"]  = meta["raw"].copy()
                st.session_state["weights_norm"] = normalize(meta["raw"])
                st.success("프리셋을 적용했습니다. ‘가중치 설정’ 페이지에서 즉시 반영됩니다.")
        with right:
            nw = normalize(meta["raw"])
            w_df = pd.DataFrame({"component": list(nw.keys()), "weight": list(nw.values())})
            pie = alt.Chart(w_df).mark_arc(innerRadius=60).encode(
                theta="weight:Q", color="component:N", tooltip=["component","weight"]
            ).properties(height=260)
            st.altair_chart(pie, use_container_width=True)

try:
    st.page_link("pages/01_Weights_Settings.py", label="가중치 설정 페이지로 이동")
except Exception:
    st.info("좌측 Pages 메뉴에서 ‘가중치 설정’을 선택해 주십시오.")


import streamlit as st


st.markdown("<h1 style='text-align: center; color: grey;'>FLOX</h1>", unsafe_allow_html=True)
st.divider()

page_element="""
<style>
[data-testid="stAppViewContainer"]{
  background-image: url("https://media.istockphoto.com/id/1329715338/vector/dots-and-lines-penetrate-upward-through-particle-trajectory-network-technology-and-speed.jpg?s=612x612&w=0&k=20&c=p4Bb2XUHc_Wl7rVMmHjj-yggeaxbcHk_Gjd4IgKu8uA=");
  background-size: cover;
}
</style>
"""

st.markdown(page_element, unsafe_allow_html=True)
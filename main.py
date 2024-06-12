import streamlit as st
from lib import * 

# Set the title of the Streamlit app
st.title("Crypto Quant")

# Add a text input field
input_data = st.text_input("Enter crypto token")

if st.button('Get Results'):
    # Check if the input field is not empty
    if input_data:
        try:
            with st.spinner("Generating graph..."):
                # Parse the input data
                data_points = main(input_data.lower())

                if data_points:
                
                    zones(data_points)
                else:
                    st.error("No data found for the given token.")
            
        except ValueError:
            st.error("Please enter a valid list of numbers separated by commas.")

    else:
        st.write("Enter some data points to see the graph.")

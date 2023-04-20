import streamlit as st
import snowflake.connector
import pandas as pd
import numpy as np

##################### Snowflake #####################


@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )


@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [i[0] for i in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


####################################################


@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')



def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["pw"] == st.secrets["pw"]:
            st.session_state["password_correct"] = True
            del st.session_state["pw"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="pw"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="pw"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True




if __name__ == "__main__":
    st.set_page_config(
        page_title="Google Photo Failures",
        page_icon="📷",
    )
    if check_password():

        st.title("Find Failed Google Photos")

        conn = init_connection()

        with st.form("Form"):

            option = st.radio(
                "Search By",
                ('Business ID', 'Entity ID'))

            id = st.text_input("Enter ID:")

            form_submitted = st.form_submit_button("Find Photos")

        if form_submitted:
            if option == 'Business ID':
                query = f'''
                    with failed_urls as
                    (
                        select distinct entity_id, photo_url, type, error_text
                        from "PROD_LISTINGS_LOCAL"."PUBLIC"."GOOGLE_PHOTO_ERRORS"
                        where business_id = {id}
                    )
                    select to_varchar(pfd.entity_id) as "Entity ID", f.photo_url, f.type, f.error_text
                    from failed_urls f
                    join "PROD_KNOWLEDGE"."PUBLIC"."PROFILE_FIELD_DATA_BY_BUSINESS" pfd on pfd.entity_id = f.entity_id
                    where pfd.field_id = 'location.gallery'
                    and contains(to_varchar(pfd.field_raw_value), substr(photo_url, 8));
                '''
            else:
                query = f'''
                    with failed_urls as
                    (
                        select distinct entity_id, photo_url, type, error_text
                        from "PROD_LISTINGS_LOCAL"."PUBLIC"."GOOGLE_PHOTO_ERRORS"
                        where entity_id = {id}
                    )
                    select to_varchar(pfd.entity_id) as "Entity ID", f.photo_url, f.type, f.error_text
                    from failed_urls f
                    join "PROD_KNOWLEDGE"."PUBLIC"."PROFILE_FIELD_DATA_BY_BUSINESS" pfd on pfd.entity_id = f.entity_id
                    where pfd.field_id = 'location.gallery'
                    and contains(to_varchar(pfd.field_raw_value), substr(photo_url, 8));
                '''

            df = run_query(query)

            csv = convert_df(df)

            st.download_button(
                label="Download All",
                data=csv,
                file_name='result.csv',
                mime='text/csv',
            )


            for index, row in df.iterrows():
                st.image(row['PHOTO_URL'])
                st.markdown(f'''
                **Entity ID**: {row['Entity ID']}  [(View)](https://www.yext.com/s/me/entity/edit3?entityIds={row['Entity ID']})  
                **Error Type**: {row['TYPE']}  
                **Error Reason**: {row['ERROR_TEXT']}  
                **Photo URL**:  {row['PHOTO_URL']}
                ''')


                
                st.divider()

                    

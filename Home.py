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


if __name__ == "__main__":
    st.set_page_config(
        page_title="Google Photo Failures",
        page_icon="📷",
    )
    st.title("Find Failed Google Photos")

    conn = init_connection()

    with st.form("Form"):

        option = st.radio(
            "Search By",
            ('Business ID', 'Entity ID'))

        id = st.text_input("Enter ID:")

        form_submitted = st.form_submit_button("Find Photo Failures")

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

        #df

        for index, row in df.iterrows():
            st.write(row)
            st.image(row['PHOTO_URL'])

        #st.image('http://a.mktgcdn.com/p/9wMZHTZSnXMrfeSfUwXEAo-Wl4xcSKi8AUJvbFpOH2Q/417x417.png')

import streamlit as st
import base64
import time
from io import BytesIO
from PIL import Image
import pandas as pd
import html
import requests
import json  # for JSON serialization if needed
    

# Set page config first
st.set_page_config(layout="wide", page_title="Supply Chain Digital Assistant")

# Session state for page navigation
if 'page' not in st.session_state:
    st.session_state.page = "home"
if 'chat_messages' not in st.session_state:
    st.session_state.chat_messages = []
if 'initial_messages_shown' not in st.session_state:
    st.session_state.initial_messages_shown = False

# Your CSS code here (keep your existing CSS)
st.markdown("""
<style>
/* Your existing CSS */
</style>
""", unsafe_allow_html=True)

# Functions for pages
def show_home_page():
    logo = Image.open("images/Bristlecone Logo.png")
    ai_chip = Image.open("images/microchip-ai.png")
    check_icon = Image.open("images/checkmark.png")
    threads_icon = Image.open("images/chat.png")
 
     
    # tmp=tempfile.NamedTemporaryFile(delete=False,suffix=".png")
    # check_icon.save(tmp.name)
    # check_icon_path=tmp.name
    buffer = BytesIO()
    logo.save(buffer, format="PNG")
    logo_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
    logo_path = f"data:image/png;base64,{logo_str}"
    
    buffer = BytesIO()
    threads_icon.save(buffer, format="PNG")
    threads_icon_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
    threads_icon_url = f"data:image/png;base64,{threads_icon_str}"
    
    buffer = BytesIO()
    check_icon.save(buffer, format="PNG")
    check_icon_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
    check_icon_path = f"data:image/png;base64,{check_icon_str}"
     
    buffer = BytesIO()
    ai_chip.save(buffer, format="PNG")
    ai_chip_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
    ai_chip_data_url = f"data:image/png;base64,{ai_chip_str}"
     
    # st.set_page_config(layout="wide", page_title="Supply Chain Digital Assistant")  
    # Add this to your existing CSS in st.markdown
    st.markdown("""
    <style>
    /* Hide the default sidebar */
    [data-testid="collapsedControl"] {
        display: none;
    }
    
    /* Hide the sidebar content when expanded */
    section[data-testid="stSidebar"] {
        display: none;
    }
    
    /* Additional style to ensure the sidebar is fully hidden */
    .css-1d391kg, .css-1vq4p4l {
        visibility: hidden;
    }
    </style>
    
    <script>
    // JavaScript to ensure sidebar stays hidden
    document.addEventListener('DOMContentLoaded', function() {
        // Hide any sidebar elements that might appear
        const sidebarElements = document.querySelectorAll('[data-testid="stSidebar"]');
        sidebarElements.forEach(function(element) {
            element.style.display = 'none';
        });
        
        // Also hide the collapse control button
        const collapseControls = document.querySelectorAll('[data-testid="collapsedControl"]');
        collapseControls.forEach(function(control) {
            control.style.display = 'none';
        });
    });
    </script>
    """, unsafe_allow_html=True)
    st.markdown(f"""
    <style>
    /* Hide default Streamlit header */
    header {{
      visibility: hidden;
    }}
    
    /* Custom header wrapper spanning full width */
    .custom-header {{
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      display: flex;
      flex-direction: row;
      align-items: center;
      justify-content: space-between;
      z-index: 10000;
      height: 90px;
      margin: 0;
    }}
    
    /* Left column (black background) */
    .custom-header-left {{
      background-color: #000000;
      width: 25%;
      padding: 0.5rem 1.5rem;
      display: flex;
      align-items: center;
      height: 90px;
    }}
    
    /* Right column (#2B2B2B background) */
    .custom-header-right {{
      background-color: #2B2B2B;
      width: 75%;
      padding: 0.5rem 1.5rem;
      display: flex;
      align-items: right;
      justify-content: flex-start;
      height:90px
    }}
    
    /* Title styling */
    .custom-header-right h1 {{
      color: white;
      font-size: 1.5rem;
      font-weight: 600;
      margin: 0;
      padding: 0;
    }}
    
    /* Push the main app content below the new header */
    .stApp {{
      padding-top: 60px;
    }}
    </style>
    
    <div class="custom-header">
      <!-- Left col: black background + Bristlecone logo -->
      <div class="custom-header-left">
        <img src="{logo_path}" width="280"/>
      </div>
    
      <!-- Right col: #2B2B2B background + title text -->
      <div class="custom-header-right">
        <h1>SUPPLY CHAIN DIGITAL ASSISTANT</h1>
      </div>
    </div>
    """, unsafe_allow_html=True)
    
    ##css code
    st.markdown(
        """
        <style>
        /* Hide default Streamlit header and footer
        #MainMenu, header, footer {visibility: hidden;}
        html,body{
          margin : 0;
          padding :0;
          height : 100%;
        }
        */
        /* Overall body background can be set if desired, but we mainly use columns */
        .main, .block-container {
            padding:0;
            height:100vh;
            max-width:100%;
        }
     
        /* Left column styling (thick black) */
        .left-col {
            background-color: #000000;  /* pure black */
            padding: 1rem;
            height:100vh;
            position:fixed;
            left:0;
            width:25%;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
        }
     
        /* Right column styling (light black / dark gray) */
        .right-col {
            background-color: #2B2B2B;
            padding: 1rem 2rem;
            height:200vh;
            position:fixed;
            overflow-y: auto;
            margin-left:25%;
            width:75%;
            padding:1.5rem;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
        }
        .content-wrapper {
            display: flex;
            height:100%;
            flex-direction: row;
        }
     
        /* Bristlecone logo */
     
     
        /* "Threads" area */
        .threads-container {
            margin-top: 2rem;
            display: flex;
            align-items: center;
            color: white;
            font-size: 1rem;
        }
        .threads-icon {
            width: 24px;
            margin-right: 0;
        }
     
        /* Title in right column */
        .title-text {
            color: white;
            font-size: 1.5rem;
            font-weight: 600;
            margin: 0;
            padding: 0;
            margin-bottom: 1rem;
        }
     
        .ai-icon {
            width: 25px;
            margin-right: 0.5rem;
        }
     
        /* Container for the three feature boxes */
        .features-container {
            display: flex;
            justify-content: center;
            gap: 0.5rem !important;
            margin-left: 2cm;
            max-width:900px;
            margin: 2rem auto 0;
            margin-top: 4rem;
        }
     
        /* Individual feature box */
        .feature-box {
            background-color: #1E1E1E;
            margin:0 !important;
            padding:1rem;
            width: 250px;
            min-height: 190px;
            border-radius: 10px;
            text-align: left;
            margin-bottom:2rem;
        }
        .feature-title {
            color: #00FF00; /* Green text */
            font-weight: 600;
            margin-bottom: 0.5rem;
        }
        .feature-desc {
            color: white; /* White text */
            font-size: 0.9rem;
            margin-bottom: 1rem;
        }
        .check-icon {
            width: 20px;
        }
        /* Top row inside right column */
        .top-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
        }
     
        </style>
        """,
        unsafe_allow_html=True
    )
     
     
    st.markdown("""
    <div class="content-wrapper">
        <div class="left-col">
            <!-- Left column content -->
        </div>
        <div class="right-col">
            <!-- Right column content -->
        </div>
    </div>
    """, unsafe_allow_html=True)
     
    # ----- LEFT COLUMN -----
    left_col = st.container()
    with left_col:
        st.markdown('<div class="left-col" style="position:fixed;">', unsafe_allow_html=True)
        st.markdown("<br/>", unsafe_allow_html=True)
        st.markdown("<br/>", unsafe_allow_html=True)
        st.markdown(
            f"""
            <div style="position:fixed;">
              <img src="{threads_icon_url}" width="24"/>
              <span style="color:gray;font-size:1rem;position:fixed;margin-left: 10px;">THREADS</span>
            </div>
            """,
            unsafe_allow_html=True
        )
        st.markdown('</div>', unsafe_allow_html=True)
     
    # ----- RIGHT COLUMN  -----
     
    with st.container():
        st.markdown(
            '<h3 class="title-text" style="margin-left: 10cm; margin-top: -3.5cm;margin-bottom: 1rem;color: white;">SUPPLY CHAIN DIGITAL ASSISTANT</h3>',
            unsafe_allow_html=True
        )
     
        # Get Started button below title
    #     st.markdown(f'''
    #     <div style="text-align: center; margin-top: 20px;">
    #         <button class="get-started-btn" id="get-started-btn">
    #             <img src="{ai_chip_data_url}" style="width: 20px; vertical-align: middle; margin-right: 10px;"/>
    #             Get Started
    #         </button>
    #     </div>
    # ''', unsafe_allow_html=True)
    st.markdown("""
        <style>
        .get-started-btn {
            background-color: #1E1E1E;
            border: 1px solid #1E1E1E;
            padding: 1rem 1.5rem;
            color: white;
            font-size: 1rem;
            border-radius: 10px;
            display: inline-flex;
            align-items: center;
            cursor: pointer;
            text-decoration: none;
            margin-top: 1rem;
        }
        .get-started-btn:hover {
            background-color: #444444;
        }
        </style>
    """, unsafe_allow_html=True)
    st.markdown("""
    <style>
    /* Target Streamlit button elements */
    div.stButton > button {
        background-color: #1E1E1E;  /* Dark background matching the image */
        color: #00C4CC;            /* Light teal text */
        border: 1px solid #1E1E1E; /* Border matching background */
        padding: 1rem 1.5rem;    /* Increased padding for larger size */
        font-size: 1.1rem;         /* Slightly larger text */
        border-radius: 10px;       /* Rounded corners */
        display: inline-flex;      /* Flexbox for alignment */
        align-items: center;       /* Center items vertically */
        position: relative;        /* For positioning the image */
    }
    div.stButton > button:hover {
        background-color: #444444; /* Hover effect */
    }
    /* Style for the image next to the button */
    .button-image {
        margin-right: 0.5rem;     /* Space between image and text */
        vertical-align: middle;   /* Align with text */
        margin-top: 0cm;
        margin-left:1.3cm;/* Shift image down by 0.2 cm */
        width: 30px;              /* Match image size from your design */
        height: 30px;             /* Match image size from your design */
    }
    </style>
""", unsafe_allow_html=True)
    
    # Hidden Streamlit Button for Navigation with Image
    cols = st.columns(3)
    with cols[2]:
        # Display the image and button side by side
        st.markdown(
            f'<img src="{ai_chip_data_url}" class="button-image">',
            unsafe_allow_html=True
        )
        # Button with original functionality
        if st.button("Get Started", key="hidden_get_started"):
            st.session_state.page = "sample"
            st.rerun()

    # JavaScript to trigger the hidden button when styled button is clicked
        # st.markdown("""
        #     <script>
        #         document.getElementById('styled-button').addEventListener('click', function() {
        #             document.querySelector('button[kind="secondaryFormSubmit"]').click();
        #         });
        #     </script>
        #     """, unsafe_allow_html=True
        # )
     
    # JavaScript to Trigger the Hidden Streamlit Button
    # st.markdown("""
    #     <script>
    #         document.getElementById('get-started-btn').addEventListener('click', function() {
    #             var hiddenBtn = window.parent.document.querySelector('[aria-label="Hidden Get Started"]');
    #             if (hiddenBtn) hiddenBtn.click();
    #         });
    #     </script>
    # """, unsafe_allow_html=True)
        # Create feature boxes with custom gap and right shift
    st.markdown('''
            <div style="display: flex; gap: 2rem; margin-left: 12cm;margin-top: 1cm;">
                <div class="feature-box style="width: 150px; min-height: 250px;">
                    <div class="feature-title">Information Retrieval</div>
                    <div class="feature-desc">
                        This feature will retrieve the required information and provide visualization.
                    </div>
                    <img src="{check_icon_path}" class="check-icon" />
                </div>
                <div class="feature-box style="width: 150px; min-height: 250px;">
                    <div class="feature-title">Smart Insights</div>
                    <div class="feature-desc">
                        This feature will generate insights by analysing the underlying data.
                    </div>
                    <img src="{check_icon_path}" class="check-icon" />
                </div>
                <div class="feature-box style="width: 150px; min-height: 250px;">
                    <div class="feature-title">Simulation</div>
                    <div class="feature-desc">
                        This feature allows users to run simulations to understand various business what-if scenarios.
                    </div>
                    <img src="{check_icon_path}" class="check-icon" />
                </div>
            </div>
        '''.format(check_icon_path=check_icon_path), unsafe_allow_html=True)

def show_sample_page():
  
  
    # ----------------------------------------------------
    #               IMAGE SETUP (LOGO, THREADS)
    # ----------------------------------------------------
    logo = Image.open("images/Bristlecone Logo.png")
    threads_icon = Image.open("images/chat.png")
    
    buffer = BytesIO()
    logo.save(buffer, format="PNG")
    logo_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
    logo_path = f"data:image/png;base64,{logo_str}"
    
    buffer = BytesIO()
    threads_icon.save(buffer, format="PNG")
    threads_icon_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
    threads_icon_url = f"data:image/png;base64,{threads_icon_str}"
    
    # ----------------------------------------------------
    #           STREAMLIT PAGE CONFIG
    # ----------------------------------------------------
    # st.set_page_config(layout="wide", page_title="Supply Chain Digital Assistant")
    
    # ----------------------------------------------------
    #      SESSION STATE INITIALIZATION
    # ----------------------------------------------------
    if 'chat_messages' not in st.session_state:
        st.session_state.chat_messages = []
    if 'initial_messages_shown' not in st.session_state:
        st.session_state.initial_messages_shown = False
    
    # ----------------------------------------------------
    #                  CUSTOM CSS
    # ----------------------------------------------------
    st.markdown("""
    <style>
    /* Hide the default sidebar */
    [data-testid="collapsedControl"] {
        display: none;
    }
    
    /* Hide the sidebar content when expanded */
    section[data-testid="stSidebar"] {
        display: none;
    }
    
    /* Additional style to ensure the sidebar is fully hidden */
    .css-1d391kg, .css-1vq4p4l {
        visibility: hidden;
    }
    </style>
    
    <script>
    // JavaScript to ensure sidebar stays hidden
    document.addEventListener('DOMContentLoaded', function() {
        // Hide any sidebar elements that might appear
        const sidebarElements = document.querySelectorAll('[data-testid="stSidebar"]');
        sidebarElements.forEach(function(element) {
            element.style.display = 'none';
        });
        
        // Also hide the collapse control button
        const collapseControls = document.querySelectorAll('[data-testid="collapsedControl"]');
        collapseControls.forEach(function(control) {
            control.style.display = 'none';
        });
    });
    </script>
    """, unsafe_allow_html=True)
    st.markdown(f"""
    <style>
    /* Hide default Streamlit header */
    header {{
      visibility: hidden;
    }}
    
    /* Custom header wrapper spanning full width */
    .custom-header {{
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      display: flex;
      flex-direction: row;
      align-items: center;
      justify-content: space-between;
      z-index: 10000;
      height: 90px;
      margin: 0;
    }}
    
    /* Left column (black background) */
    .custom-header-left {{
      background-color: #000000;
      width: 25%;
      padding: 0.5rem 1.5rem;
      display: flex;
      align-items: center;
      height: 90px;
    }}
    
    /* Right column (#2B2B2B background) */
    .custom-header-right {{
      background-color: #2B2B2B;
      width: 75%;
      padding: 0.5rem 1.5rem;
      display: flex;
      align-items: right;
      justify-content: flex-start;
      height:90px
    }}
    
    /* Title styling */
    .custom-header-right h1 {{
      color: white;
      font-size: 1.5rem;
      font-weight: 600;
      margin: 0;
      padding: 0;
    }}
    
    /* Push the main app content below the new header */
    .stApp {{
      padding-top: 60px;
    }}
    </style>
    
    <div class="custom-header">
      <!-- Left col: black background + Bristlecone logo -->
      <div class="custom-header-left">
        <img src="{logo_path}" width="280"/>
      </div>
    
      <!-- Right col: #2B2B2B background + title text -->
      <div class="custom-header-right">
        <h1>SUPPLY CHAIN DIGITAL ASSISTANT</h1>
      </div>
    </div>
    """, unsafe_allow_html=True)
    
    
    st.markdown(
        """
        <style>
        .main, .block-container {
            padding:0;
            height:100vh;
            max-width:100%;
        }
        .left-col {
            background-color: #000000;
            padding: 1rem;
            height:100vh;
            position:fixed;
            left:0;
            width:25%;
            overflow-y: hidden;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
        }
        .right-col {
            background-color: #2B2B2B;
            padding: 1.5rem;
            height:100vh;
            position:fixed;
            overflow-x: auto;
            margin-left:25%;
            width:75%;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
        }
        .content-wrapper {
            display: flex;
            height:100%;
            flex-direction: row;
        }
        .title-text {
            color: white;
            position: fixed;
            font-size: 1.5rem;
            font-weight: 600;
            margin: 0;
            padding: 0;
            margin-bottom: 1rem;
        } 
         .chat-container {
           margin-top: 60px;
           height: calc(100vh - 230px);
           overflow-y: auto !important;
           position: relative;
           z-index: 1;
           padding-bottom: 100px !important;
        }
        .assistant-message {
            display: flex;
            align-items: flex-start;
            margin-left: 380px;
            margin-bottom: 20px;
            z-index:1;
        }
        .assistant-avatar {
            width: 40px;
            height: 40px;
            border-radius: 50%;
            background: linear-gradient(135deg, #1cd8d2 0%, #93edc7 100%);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            margin-right: 10px;
            color: #FFFFFF;
            z-index:1;
        }
        .assistant-text {
            background-color: #3A3A3A;
            padding: 10px;
            border-radius: 5px;
            max-width: 60%;
            color: #FFFFFF;
            z-index:1;
        }
        .user-message {
            display: flex;
            justify-content: flex-end;
            margin-bottom: 20px;
            z-index:1;
        }
        .user-text {
            background-color: #444444;
            padding: 10px;
            border-radius: 5px;
            max-width: 60%;
            color: #FFFFFF;
            z-index:1;
        }
        .typing {
            color: #AAAAAA;
            font-style: italic;
            margin-left: 380px;
            margin-bottom: 10px;
            z-index:1;
        }
        .input-container {
            position: fixed;
            bottom: 0;
            margin-left: 25%;
            width: 75%;
            background-color: #2B2B2B;
            padding: 10px;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        .input-box {
            width: 80%;
            padding: 8px;
            border-radius: 4px;
            border: none;
            outline: none;
            margin-right: 10px;
            background-color: #000000 !important;
            color: #FFFFFF;
        }
        .input-box::placeholder {
            color: gray;
        }
        .send-button {
            padding: 8px 16px;
            border-radius: 4px;
            background-color: #000000;
            color: white;
            border: none;
            cursor: pointer;
        }
        .stForm {
          position: fixed !important;
          bottom: 0 !important;
          left: 25% !important;
          width: 75% !important;
          z-index: 1001 !important;
          background-color: #2B2B2B !important;
          padding: 20px !important;
          border-top: 2px solid #444 !important;
        }
        .table-container {
          margin-left: 380px;
          max-width: calc(100% - 450px);
          overflow-x: auto;
          margin-bottom: 20px;
          background-color: #3A3A3A;
          border-radius: 5px;
          padding: 10px;
        }
        .table-container table {
            width: auto;
            min-width: 100%;
            background-color: #000000 !important;
            color: white !important;
            border-color: #000000 !important;
            border-collapse: collapse;
        }
        .table-container th, .table-container td {
            background-color: #000000 !important;
            color: white !important;
            border: 1px solid #000000 !important;
            padding: 8px;
            text-align: left;
        }
        .table-container td {
            background-color: #2B2B2B !important;
            color: white !important;
            border: 1px solid #000000 !important;
            padding: 8px;
            text-align: left;
        }
        .table-container tr:nth-child(odd) td {
          background-color: #333333 !important;
        }
        .table-container tr:hover td {
          background-color: #444444 !important;
        }
        .stMarkdown {
           all: unset;
        }
        .element-container .stMarkdown {
           all: unset;
        }
        .stApp {
          padding-top: 60px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )
    
    # ----------------------------------------------------
    #                LAYOUT STRUCTURE
    # ----------------------------------------------------
    st.markdown("""
    <div class="content-wrapper">
        <div class="left-col">
        </div>
        <div class="right-col">
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # -------------------- LEFT COLUMN --------------------
    left_col = st.container()
    with left_col:
        st.markdown('<div class="left-col" style="position:fixed;">', unsafe_allow_html=True)
        st.markdown("<br/>", unsafe_allow_html=True)
        st.markdown("<br/>", unsafe_allow_html=True)
        st.markdown(
            f"""
            <div style="position:fixed;">
              <img src="{threads_icon_url}" width="24"/>
              <span style="color:gray;font-size:1rem;position:fixed;margin-left: 10px;">THREADS</span>
            </div>
            """,
            unsafe_allow_html=True
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    # -------------------- CHAT CONTAINER -----------------
    right_col = st.container()
    with right_col:
        st.markdown('<div class="right-col" style="position:fixed; margin-left:25%; width:75%;">', unsafe_allow_html=True)
        chat_container = st.container()
        with chat_container:
            st.markdown('<div class="chat-container" id="chat-container" style="margin-top: -20cm;">', unsafe_allow_html=True)
    
            # Display existing messages
            for idx, message in enumerate(st.session_state.chat_messages):
                if message["role"] == "assistant":
                    if isinstance(message["content"], dict):
                        text = message["content"].get("text", "")
                        table_data = message["content"].get("table_data", [])
                        # Here is your base64 from the backend's "image" field.
                        # We'll store it as "graph_base64" in the combined_content below.
                        graph_base64 = message["content"].get("graph_base64", None)
    
                        plain_text = str(text).replace('<', '&lt;').replace('>', '&gt;')
    
                        # Display the text message
                        st.markdown(
                            f"""
                            <div class="assistant-message" style="margin-top: 5px;">
                                <div class="assistant-avatar">S</div>
                                <div class="assistant-text">{plain_text}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
    
                        # Display table if present
                        if table_data and isinstance(table_data, (list, dict)):
                            try:
                                if isinstance(table_data, dict):
                                    table_data = [table_data]
                                table_data = [item for item in table_data if isinstance(item, dict)]
                                if table_data:
                                    df = pd.DataFrame(table_data)
                                    table_id = f"table_{hash(str(table_data))}"
                                    html_table = f"""
                                    <div class="table-container">
                                        <table id="{table_id}" class="display">
                                            <thead>
                                                <tr>
                                                    {"".join(f"<th>{col}</th>" for col in df.columns)}
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {"".join(
                                                    f"<tr>{''.join(f'<td>{html.escape(str(cell))}</td>' for cell in row)}</tr>"
                                                    for _, row in df.iterrows()
                                                )}
                                            </tbody>
                                        </table>
                                    </div>
                                    """
                                    st.markdown(html_table, unsafe_allow_html=True)
                                    st.markdown("""
                                      <style>
                                      .stButton > button {
                                          background-color: #2B2B2B !important;  /* Dark gray background */
                                          color: #0000FF !important;            /* Blue text */
                                          border: none !important;              /* No border */
                                          border-radius: 5px;                   /* Maintains rounded edges */
                                      }
                                      </style>
                                  """, unsafe_allow_html=True)
                                    if graph_base64:
                                    
                                        graph_button_key = f"view_graph_button_{idx}"
                                        cols = st.columns([1, 4])
                                        with cols[1]:
                                          sub_cols = st.columns([1, 9])
                                          with sub_cols[1]:
                                              if st.button("View Graph", key=graph_button_key):
                                                  with st.container():
                                                      st.image(f"data:image/png;base64,{graph_base64}",width=500)
                            except Exception as e:
                                st.markdown(
                                    f"""
                                    <div class="assistant-message" style="margin-top: 5px;">
                                        <div class="assistant-avatar">S</div>
                                        <div class="assistant-text">Error rendering table: {str(e).replace('<','&lt;').replace('>','&gt;')}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )
    
                    else:
                        # Just text
                        plain_content = str(message["content"]).replace('<', '&lt;').replace('>', '&gt;')
                        st.markdown(
                            f"""
                            <div class="assistant-message" style="margin-top: 5px;">
                                <div class="assistant-avatar">S</div>
                                <div class="assistant-text">{plain_content}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                else:  # user message
                    plain_content = str(message["content"]).replace('<', '&lt;').replace('>', '&gt;')
                    st.markdown(
                        f"""
                        <div class="user-message">
                            <div class="user-text">{plain_content}</div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
    
            # Show initial messages only once
            if not st.session_state.initial_messages_shown:
                initial_messages = [
                    "I am your Supply Chain Digital Assistant!",
                    "I work closely with Joule",
                    "Please let me know how can I help you"
                ]
                for msg in initial_messages:
                    typing_placeholder = st.empty()
                    typing_placeholder.markdown(
                        "<div class='typing' style='margin-left: 380px;position: relative; z-index: 10;'>Assistant is typing...</div>",
                        unsafe_allow_html=True
                    )
                    time.sleep(1.0)
                    typing_placeholder.empty()
                    st.session_state.chat_messages.append({"role": "assistant", "content": msg})
                    st.markdown(
                        f"""
                        <div class="assistant-message" style="margin-top: 5px;">
                            <div class="assistant-avatar">S</div>
                            <div class="assistant-text">{msg}</div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                st.session_state.initial_messages_shown = True
    
            st.markdown('</div>', unsafe_allow_html=True)
    
        st.markdown("<div style='height: 150px;'></div>", unsafe_allow_html=True)
    
        # ----------------- USER INPUT FORM -------------------
        with st.form(key='chat_form', clear_on_submit=True):
            col1, col2 = st.columns([8, 1])
            with col1:
                user_input = st.text_input(
                    "",
                    placeholder="Start a Conversation...",
                    label_visibility="collapsed",
                    key="user_input"
                )
            with col2:
                submit_button = st.form_submit_button("Send", use_container_width=True)
    
        # -------------------- HANDLE SUBMISSION --------------
        if submit_button and user_input.strip():
            with chat_container:
                # Add user message
                st.session_state.chat_messages.append({"role": "user", "content": user_input})
                st.markdown(
                    f"""
                    <div class="user-message">
                        <div class="user-text">{user_input}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
    
                # Show typing animation
                typing_placeholder = st.empty()
                typing_placeholder.markdown(
                    "<div class='typing' style='margin-left: 380px;'>Assistant is typing...</div>",
                    unsafe_allow_html=True
                )
                time.sleep(1.0)
    
                try:
                    response = requests.post('http://localhost:5000/getdata', json={'user_question': user_input})
                    if response.status_code == 200:
                        data = response.json()
                        message = data.get('message', 'No message returned')
                        result = data.get('result', {})
                        
                        # Here is the base64 string the backend returned
                        # e.g. "data:image/png;base64,ABC..."
                        # or sometimes just the raw base64
                        image_url = data.get('image', "")  
    
                        # We remove the "data:image/png;base64," prefix if it exists (just to keep it consistent in our code)
                        # because we'll add it ourselves inside st.image(...) above
                        graph_base64 = image_url.replace("data:image/png;base64,", "")
    
                        plain_message = str(message).replace('<', '&lt;').replace('>', '&gt;')
    
                        if isinstance(result, dict):
                            result = [result]
                        elif not isinstance(result, list):
                            result = []
                        result = [item for item in result if isinstance(item, dict)]
    
                        # Store everything in session
                        combined_content = {
                            "text": plain_message,
                            "table_data": result,
                            "graph_base64": graph_base64  # <= This is key
                        }
                        st.session_state.chat_messages.append({"role": "assistant", "content": combined_content})
    
                        # Clear typing
                        typing_placeholder.empty()
    
                        # Show the assistant text
                        st.markdown(
                            f"""
                            <div class="assistant-message" style="margin-top: 5px;">
                                <div class="assistant-avatar">S</div>
                                <div class="assistant-text">{plain_message}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
    
                        # Show the table if present
                        if result:
                            try:
                                df = pd.DataFrame(result)
                                table_id = f"table_{hash(str(result))}"
                                html_table = f"""
                                <div class="table-container">
                                    <table id="{table_id}" class="display">
                                        <thead>
                                            <tr>
                                                {"".join(f"<th>{col}</th>" for col in df.columns)}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {"".join(
                                                f"<tr>{''.join(f'<td>{html.escape(str(cell))}</td>' for cell in row)}</tr>"
                                                for _, row in df.iterrows()
                                            )}
                                        </tbody>
                                    </table>
                                </div>
                                """
                                st.markdown(html_table, unsafe_allow_html=True)
                                st.markdown("""
                                      <style>
                                      .stButton > button {
                                          background-color: #2B2B2B !important;  /* Dark gray background */
                                          color: #0000FF !important;            /* Blue text */
                                          border: none !important;              /* No border */
                                          border-radius: 5px;                   /* Maintains rounded edges */
                                      }
                                      </style>
                                  """, unsafe_allow_html=True)
                                # If there's a graph, show a button that uses the newly stored base64
                                if graph_base64:
                                  cols = st.columns([1, 4])
                                  with cols[1]:
                                      sub_cols = st.columns([1, 9])
                                      with sub_cols[1]:
                                          if st.button("View Graph"):
                                              with st.container(): # Use a container to create a dedicated space
                                                  st.image(f"data:image/png;base64,{graph_base64}", width=500)
    
                            except Exception as e:
                                st.markdown(
                                    f"""
                                    <div class="assistant-message" style="margin-top: 5px;">
                                        <div class="assistant-avatar">S</div>
                                        <div class="assistant-text">Error rendering table: {str(e).replace('<','&lt;').replace('>','&gt;')}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )
    
                    else:
                        typing_placeholder.empty()
                        error_message = f"Error: Received status code {response.status_code}"
                        st.session_state.chat_messages.append({
                            "role": "assistant",
                            "content": error_message
                        })
                        st.markdown(
                            f"""
                            <div class="assistant-message" style="margin-top: 5px;">
                                <div class="assistant-avatar">S</div>
                                <div class="assistant-text">{error_message}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                except Exception as e:
                    typing_placeholder.empty()
                    error_message = f"Error connecting to API: {str(e).replace('<','&lt;').replace('>','&gt;')}"
                    st.session_state.chat_messages.append({
                        "role": "assistant",
                        "content": error_message
                    })
                    st.markdown(
                        f"""
                        <div class="assistant-message" style="margin-top: 5px;">
                            <div class="assistant-avatar">S</div>
                            <div class="assistant-text">{error_message}</div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
    
            # Auto-scroll
            st.markdown(
                """
                <script>
                function scrollChatToBottom() {
                    var chatContainer = document.getElementById('chat-container');
                    if (chatContainer) {
                        chatContainer.scrollTop = chatContainer.scrollHeight;
                    }
                }
                window.addEventListener('load', function() {
                    setTimeout(scrollChatToBottom, 500);
                });
                </script>
                """,
                unsafe_allow_html=True
            )
            st.rerun()


# Main app logic
def main():
    # Common header
    st.markdown(f"""
    <div class="custom-header">
        <!-- Your header HTML -->
    </div>
    """, unsafe_allow_html=True)
    
    # Display the appropriate page
    if st.session_state.page == "home":
        show_home_page()
    elif st.session_state.page == "sample":
        show_sample_page()

if __name__ == "__main__":
    main()

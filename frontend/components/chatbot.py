"""Right panel: Chat interface for interacting with agents."""

from __future__ import annotations

import streamlit as st

from frontend import api_client


def render_chatbot():
    """Render the chatbot interface."""
    st.markdown("### Assistant")

    # Display chat history
    chat_container = st.container(height=500)
    with chat_container:
        messages = st.session_state.get("chat_messages", [])
        for msg in messages:
            role = msg.get("role", "assistant")
            with st.chat_message(role):
                st.markdown(msg["content"])

    # Show interrupt prompt if waiting for user input
    interrupt_data = st.session_state.get("interrupt_data")
    if interrupt_data:
        with chat_container:
            with st.chat_message("assistant"):
                st.markdown(interrupt_data.get("message", "Awaiting your input..."))

    # Chat input
    session_id = st.session_state.get("session_id")
    phase = st.session_state.get("phase", "upload")

    if session_id and interrupt_data:
        # Agent is waiting for user input (approval, feedback, etc.)
        if prompt := st.chat_input("Type your response..."):
            st.session_state.chat_messages.append({"role": "user", "content": prompt})
            st.session_state.interrupt_data = None
            st.session_state.polling = True
            try:
                api_client.send_message(session_id, prompt)
            except Exception as e:
                st.session_state.chat_messages.append({
                    "role": "assistant",
                    "content": f"Error: {str(e)}",
                })
            st.rerun()

    elif session_id and phase not in ("upload", "complete", "error"):
        # Agent is working — keep input visible but disabled so the UI doesn't jump
        st.chat_input("Working on it...", disabled=True)

    elif phase == "complete":
        if prompt := st.chat_input("Ask about your data or start a new session..."):
            st.session_state.chat_messages.append({"role": "user", "content": prompt})
            st.rerun()

    else:
        # No session yet — first message starts the whole pipeline
        if prompt := st.chat_input(
            "Describe your tables (e.g. 'users, orders, products') or paste a SQL schema..."
        ):
            st.session_state.chat_messages.append({"role": "user", "content": prompt})
            try:
                new_session_id = api_client.create_session()
                st.session_state.session_id = new_session_id
                api_client.upload_schema(new_session_id, prompt)
                st.session_state.phase = "parsing"
                st.session_state.polling = True
                st.session_state.poll_error_count = 0
            except Exception as e:
                st.session_state.chat_messages.append({
                    "role": "assistant",
                    "content": f"Could not start session: {str(e)}",
                })
            st.rerun()

# GCP-RAG-Chatbot-Inventory-Management
AI-Powered Inventory Chatbot using Google Cloud's RAG Engine


A complete, end-to-end project demonstrating how to build a conversational AI chatbot for querying real-time inventory data. This solution is built entirely within Google Cloud's Vertex AI Search and Conversation platform. It uses a Search App for its Retrieval-Augmented Generation (RAG) core, a Cloud Function for automated data transformation, and a Chat App for the conversational interface. 

The core of the project is a sophisticated Cloud Function that performs an ETL-like process: it ingests multiple related CSVs (items, branches, sales), performs an in-memory join to enrich the sales data with item and branch names, and transforms the structured data into a natural language format suitable for the RAG engine. This makes complex relational data, such as "Which items were sold at which branch?", accessible through simple, natural language questions.

"""Governance Module.

Sub-packages are imported lazily within each module to avoid pulling
heavy dependencies (boto3, LLM, Bedrock) at package level.
"""

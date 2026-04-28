import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-me-for-production")
SQLALCHEMY_TRACK_MODIFICATIONS = False
PUBLIC_ROLE_LIKE = None
AUTH_ROLE_PUBLIC = "Public"
AUTH_USER_REGISTRATION = False
GUEST_ROLE_NAME = None
FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,
}
NEXUS_POLICY_ROLES = {
    "admin": ["Admin"],
    "steward": ["Admin", "Alpha"],
    "analyst": ["Gamma"],
    "public": [],
}

# Add a Trino database connection in the Superset UI:
# trino://nexus@trino:8080/iceberg

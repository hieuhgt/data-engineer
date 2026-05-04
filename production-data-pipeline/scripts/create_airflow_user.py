"""
Create an Airflow admin user directly via FAB SQLAlchemy models,
bypassing the broken `airflow users create` CLI (find_role bug in
apache-airflow-providers-fab for Airflow 3.x).

Run inside the scheduler container:
  docker exec -it pipeline-airflow-scheduler python /opt/airflow/src/../scripts/create_airflow_user.py
"""

import os
import sys

os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow",
)

from airflow.www.app import create_app  # noqa: E402

app = create_app()

with app.app_context():
    from airflow.www.security import AirflowSecurityManager  # noqa: E402

    sm = app.appbuilder.sm

    # Find or create Admin role
    admin_role = sm.find_role("Admin")
    if admin_role is None:
        admin_role = sm.add_role("Admin")
        print("Created 'Admin' role")
    else:
        print("Found 'Admin' role")

    username = "admin"
    existing = sm.find_user(username=username)
    if existing:
        print(f"User '{username}' already exists — skipping creation")
        sys.exit(0)

    user = sm.add_user(
        username=username,
        first_name="Admin",
        last_name="User",
        email="admin@example.com",
        role=admin_role,
        password="admin",
    )

    if user:
        print(f"Successfully created user '{username}' with role 'Admin'")
    else:
        print("Failed to create user — check logs above")
        sys.exit(1)

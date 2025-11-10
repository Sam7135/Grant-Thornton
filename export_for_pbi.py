import pandas as pd
from sqlalchemy import create_engine

ENGINE = create_engine(
    "mysql+pymysql://portfolio_user:StrongPass123!@localhost:3306/portfolio",
    future=True
)

def main():
    flags = pd.read_sql("SELECT * FROM v_payroll_flags", ENGINE)
    flags["Flag"] = (flags["Underpaid"] > 1).map({True:"Yes", False:"No"})
    flags.to_csv("payroll_audit_summary.csv", index=False)

    emps = pd.read_sql("SELECT EmployeeID, Name FROM employees", ENGINE)
    emps.to_csv("employees.csv", index=False)

    print("✅ Exported payroll_audit_summary.csv and employees.csv")

if __name__ == "__main__":
    main()

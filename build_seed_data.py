import random
from datetime import datetime
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine

# Update only if you changed the DB/user/pass/port
ENGINE = create_engine(
    "mysql+pymysql://portfolio_user:StrongPass123!@localhost:3306/portfolio",
    future=True
)

def main():
    fake = Faker(); Faker.seed(42); random.seed(42)

    # --- Employees ---
    employee_ids = [f"E{str(i).zfill(3)}" for i in range(1, 51)]
    employees = pd.DataFrame({
        "EmployeeID": employee_ids,
        "Name": [fake.name() for _ in employee_ids],
        "HourlyRate": [round(random.uniform(22,35),2) for _ in employee_ids]
    })

    # --- Timesheets (weekdays Jan–Feb 2023) ---
    start, end = datetime(2023,1,1), datetime(2023,2,28)
    dates = pd.date_range(start, end, freq="D")
    rows=[]
    for emp in employee_ids:
        for d in dates:
            if d.weekday() < 5:  # Mon–Fri
                hours = random.choices([7,8,9,10], weights=[10,50,25,15])[0]
                rows.append([emp, d.date(), hours])
    timesheet = pd.DataFrame(rows, columns=["EmployeeID","WorkDate","HoursWorked"])

    # --- Payroll (biweekly; inject 10% underpayment) ---
    pay_periods = pd.date_range(start, end, freq="2W-MON")
    pr=[]
    for _, emp in employees.iterrows():
        rate = emp["HourlyRate"]
        for i in range(len(pay_periods)-1):
            p0, p1 = pay_periods[i].date(), pay_periods[i+1].date()
            mask = (
                (timesheet.EmployeeID==emp.EmployeeID) &
                (timesheet.WorkDate>=p0) & (timesheet.WorkDate<p1)
            )
            total = float(timesheet.loc[mask,"HoursWorked"].sum())
            reg, ot = (76, total-76) if total>76 else (total, 0)
            gross = round(reg*rate + ot*rate*1.5, 2)
            if random.random() < 0.10:
                gross = round(gross - random.uniform(20,100), 2)
            pr.append([emp.EmployeeID, p0, p1, rate, total, gross])

    payroll = pd.DataFrame(
        pr, columns=["EmployeeID","PeriodStart","PeriodEnd","HourlyRate","TotalHours","GrossPay"]
    )

    # --- Load to MySQL (replace mode keeps dev reproducible) ---
    employees.to_sql("employees", ENGINE, if_exists="replace", index=False)
    timesheet.to_sql("timesheet", ENGINE, if_exists="replace", index=False)
    payroll.to_sql("payroll",   ENGINE, if_exists="replace", index=False)

    print("✅ Seeded: employees, timesheet, payroll → portfolio DB")

if __name__ == "__main__":
    main()

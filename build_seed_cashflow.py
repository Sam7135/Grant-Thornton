import random
from datetime import date, timedelta
from faker import Faker
import pandas as pd
from sqlalchemy import create_engine

ENGINE = create_engine("mysql+pymysql://portfolio_user:StrongPass123!@localhost:3306/portfolio", future=True)

def main():
    fake = Faker(); Faker.seed(11); random.seed(11)

    # --- Customers & Suppliers
    cust_ids = [f"C{str(i).zfill(3)}" for i in range(1, 41)]
    sup_ids  = [f"S{str(i).zfill(3)}" for i in range(1, 26)]
    customers = pd.DataFrame({
        "CustomerID": cust_ids,
        "Name": [fake.company() for _ in cust_ids],
        "RiskTier": [random.choices(["Low","Medium","High"], [60,30,10])[0] for _ in cust_ids]
    })
    suppliers = pd.DataFrame({
        "SupplierID": sup_ids,
        "Name": [fake.company() for _ in sup_ids]
    })

    # --- Invoices (last 9 months), payments (lags)
    start = pd.Timestamp.today().normalize() - pd.Timedelta(days=270)
    end   = pd.Timestamp.today().normalize()

    invoices = []
    payments = []
    inv_no = 1; pay_no = 1

    for cid in cust_ids:
        for _ in range(random.randint(6, 18)):
            issue = fake.date_between_dates(start.to_pydatetime(), end.to_pydatetime())
            terms = random.choice([14, 21, 30, 45])
            due = pd.Timestamp(issue) + pd.Timedelta(days=terms)
            amt = round(random.uniform(500, 20000), 2)
            invoices.append([f"INV{inv_no:05d}", cid, issue, due.date(), amt])
            inv_no += 1

            # payment lag distribution: some pay on time, some late, some partial
            lag_days = random.choices([0, 7, 14, 30, 45], [30, 25, 20, 15, 10])[0]
            if random.random() < 0.2:  # partial or split payments
                first = round(amt * random.uniform(0.3, 0.7), 2)
                second = round(amt - first, 2)
                pay1 = pd.Timestamp(due) + pd.Timedelta(days=lag_days)
                pay2 = pay1 + pd.Timedelta(days=random.randint(7, 30))
                payments += [
                    [f"PMT{pay_no:05d}", f"INV{inv_no-1:05d}", pay1.date(), first]
                ]
                pay_no += 1
                payments += [
                    [f"PMT{pay_no:05d}", f"INV{inv_no-1:05d}", pay2.date(), second]
                ]
                pay_no += 1
            else:
                pay = pd.Timestamp(due) + pd.Timedelta(days=lag_days)
                payments += [[f"PMT{pay_no:05d}", f"INV{inv_no-1:05d}", pay.date(), amt]]
                pay_no += 1

    invoices = pd.DataFrame(invoices, columns=["InvoiceID","CustomerID","IssueDate","DueDate","Amount"])
    payments = pd.DataFrame(payments, columns=["PaymentID","InvoiceID","PaymentDate","Amount"])

    # --- Bills (AP) and payments
    bills, billpays = [], []
    bill_no = 1; bp_no = 1
    for sid in sup_ids:
        for _ in range(random.randint(6, 16)):
            bdate = fake.date_between_dates(start.to_pydatetime(), end.to_pydatetime())
            terms = random.choice([14, 30, 45])
            due = pd.Timestamp(bdate) + pd.Timedelta(days=terms)
            amt = round(random.uniform(300, 15000), 2)
            bills.append([f"B{bill_no:05d}", sid, bdate, due.date(), amt])
            bill_no += 1

            lag_days = random.choices([0,7,14,21], [40,30,20,10])[0]
            pay = pd.Timestamp(due) + pd.Timedelta(days=lag_days)
            billpays.append([f"BP{bp_no:05d}", f"B{bill_no-1:05d}", pay.date(), amt])
            bp_no += 1

    bills     = pd.DataFrame(bills, columns=["BillID","SupplierID","BillDate","DueDate","Amount"])
    billpays  = pd.DataFrame(billpays, columns=["PayID","BillID","PayDate","Amount"])

    # --- Bank transactions (use payments and bill payments + noise)
    bank = []
    tno = 1
    # invoice receipts (inflows)
    for p in payments.itertuples():
        bank.append([f"T{tno:06d}", p.PaymentDate, f"Receipt {p.InvoiceID}", round(p.Amount,2)])
        tno += 1
    # bill payments (outflows)
    for bp in billpays.itertuples():
        bank.append([f"T{tno:06d}", bp.PayDate, f"Supplier payment {bp.BillID}", round(-bp.Amount,2)])
        tno += 1
    # regular expenses (rent, payroll approximations)
    today = pd.Timestamp.today().normalize()
    start_all = (today - pd.Timedelta(days=270)).date()
    for d in pd.date_range(start_all, today, freq="W-FRI"):
        bank.append([f"T{tno:06d}", d.date(), "Weekly operational spend", round(-random.uniform(1000, 4000),2)])
        tno += 1
    for d in pd.date_range(start_all, today, freq="MS"):
        bank.append([f"T{tno:06d}", d.date(), "Monthly rent", -4000.00])
        tno += 1

    bank = pd.DataFrame(bank, columns=["TxnID","TxnDate","Description","Amount"])

    # --- Write to MySQL
    customers.to_sql("cf_customers", ENGINE, if_exists="replace", index=False)
    suppliers.to_sql("cf_suppliers", ENGINE, if_exists="replace", index=False)
    invoices .to_sql("cf_invoices",  ENGINE, if_exists="replace", index=False)
    payments .to_sql("cf_payments",  ENGINE, if_exists="replace", index=False)
    bills    .to_sql("cf_bills",     ENGINE, if_exists="replace", index=False)
    billpays .to_sql("cf_billpayments", ENGINE, if_exists="replace", index=False)
    bank     .to_sql("cf_bank_txn",  ENGINE, if_exists="replace", index=False)

    print("✅ Seeded cash-flow data")

if __name__ == "__main__":
    main()

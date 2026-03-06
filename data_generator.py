import pandas as pd
import numpy as np
from datetime import datetime
import random
import os

os.makedirs('backend/data', exist_ok=True)

np.random.seed(42)
random.seed(42)
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 6, 30)
dates = pd.date_range(start_date, end_date, freq='D')

# Production logs with cost & profit
machines = [1, 2, 3, 4, 5]
shifts = [1, 2, 3]
records = []

for date in dates:
    for machine in machines:
        for shift in shifts:
            target = np.random.randint(80, 120)
            if machine == 3 and date.weekday() == 2:
                downtime = np.random.randint(120, 300)
            else:
                downtime = np.random.randint(0, 60)
            if date.weekday() == 0:
                employees = np.random.randint(2, 4)
            else:
                employees = np.random.randint(4, 6)
            output = int(target * (1 - downtime/(8*60)) * (employees/5))
            output = max(0, output)
            good = np.random.randint(int(output*0.85), output) if output > 0 else 0

            unit_price = np.random.uniform(100, 200)
            material_cost_per_unit = np.random.uniform(30, 80)
            labor_rate = 20
            shift_hours = 8
            labor_cost = employees * shift_hours * labor_rate
            material_cost = output * material_cost_per_unit
            downtime_cost = (downtime / 60) * 500
            total_cost = labor_cost + material_cost + downtime_cost
            revenue = output * unit_price
            profit = revenue - total_cost

            records.append([date, shift, machine, output, target, downtime, employees, good, output,
                            unit_price, material_cost_per_unit, labor_cost, material_cost,
                            downtime_cost, total_cost, revenue, profit])

prod_df = pd.DataFrame(records, columns=['date', 'shift', 'machine_id', 'output_units', 'target_units',
                                         'downtime_minutes', 'employees_present', 'quality_good_units',
                                         'quality_total_units', 'unit_price', 'material_cost_per_unit',
                                         'labor_cost', 'material_cost', 'downtime_cost', 'total_cost',
                                         'revenue', 'profit'])
prod_df.to_csv('backend/data/production_logs.csv', index=False)
print("Production logs saved.")

# Inventory logs
materials = ['RM001', 'RM002', 'RM003']
inv_records = []
for date in dates:
    for material in materials:
        stock = np.random.randint(200, 500)
        usage = np.random.randint(20, 60)
        reorder = 150
        lead_time = np.random.randint(3, 7)
        inv_records.append([date, material, stock, reorder, usage, lead_time])
inv_df = pd.DataFrame(inv_records, columns=['date', 'material_id', 'stock_quantity', 'reorder_point',
                                            'daily_usage', 'lead_time_days'])
inv_df.to_csv('backend/data/inventory_logs.csv', index=False)
print("Inventory logs saved.")

# Employee logs
employees = [101,102,103,104,105,106,107,108,109,110]
emp_records = []
for date in dates:
    for emp in employees:
        if date.weekday() == 0 and emp in [101,102,103]:
            hours = 0
        else:
            hours = np.random.randint(7, 9)
        output_contributed = np.random.randint(10, 30)
        emp_records.append([date, emp, random.choice(shifts), hours, output_contributed])
emp_df = pd.DataFrame(emp_records, columns=['date', 'employee_id', 'shift', 'hours_worked', 'output_contributed'])
emp_df.to_csv('backend/data/employee_logs.csv', index=False)
print("Employee logs saved.")

# Financial transactions
transactions = []
for date in dates:
    for _ in range(np.random.randint(3, 8)):
        amount = np.random.randint(100, 5000)
        type = np.random.choice(['purchase', 'sale', 'payroll'])
        vendor = np.random.choice(['VendorA', 'VendorB', 'VendorC', 'Internal'])
        transactions.append([date, amount, type, vendor])
    if np.random.random() < 0.1:
        transactions.append([date, np.random.randint(10000, 50000), 'purchase', 'Unknown'])
trans_df = pd.DataFrame(transactions, columns=['date', 'amount', 'type', 'vendor'])
trans_df.to_csv('backend/data/transactions.csv', index=False)
print("Financial transactions saved.")
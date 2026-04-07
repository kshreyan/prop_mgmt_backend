from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from typing import Optional
from pydantic import BaseModel
from datetime import date

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "mgmt54500-kumar"
DATASET = "property_mgmt"

# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------
class IncomeCreate(BaseModel):
    amount: float
    date: date
    description: Optional[str] = None

class ExpenseCreate(BaseModel):
    amount: float
    date: date
    category: str
    vendor: Optional[str] = None
    description: Optional[str] = None

# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------
def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------
@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT property_id, name, address, city, state, postal_code,
               property_type, tenant_name, monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """
    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    return [dict(row) for row in results]


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT property_id, name, address, city, state, postal_code,
               property_type, tenant_name, monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
    """
    try:
        results = list(bq.query(query).result())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    if not results:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    return dict(results[0])

# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------
@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    # Check property exists
    check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not check:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    query = f"""
        SELECT income_id, property_id, amount, date, description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
        ORDER BY date
    """
    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    return [dict(row) for row in results]


@app.post("/income/{property_id}", status_code=201)
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    # Check property exists
    check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not check:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    # Get next ID
    max_id = list(bq.query(f"SELECT IFNULL(MAX(income_id), 0) as max_id FROM `{PROJECT_ID}.{DATASET}.income`").result())[0]["max_id"]
    new_id = max_id + 1
    description = f"'{income.description}'" if income.description else "NULL"
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income` (income_id, property_id, amount, date, description)
        VALUES ({new_id}, {property_id}, {income.amount}, '{income.date}', {description})
    """
    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    return {"income_id": new_id, "property_id": property_id, "amount": income.amount, "date": str(income.date), "description": income.description}

# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------
@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    # Check property exists
    check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not check:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    query = f"""
        SELECT expense_id, property_id, amount, date, category, vendor, description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = {property_id}
        ORDER BY date
    """
    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    return [dict(row) for row in results]


@app.post("/expenses/{property_id}", status_code=201)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    # Check property exists
    check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not check:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    # Get next ID
    max_id = list(bq.query(f"SELECT IFNULL(MAX(expense_id), 0) as max_id FROM `{PROJECT_ID}.{DATASET}.expenses`").result())[0]["max_id"]
    new_id = max_id + 1
    vendor = f"'{expense.vendor}'" if expense.vendor else "NULL"
    description = f"'{expense.description}'" if expense.description else "NULL"
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses` (expense_id, property_id, amount, date, category, vendor, description)
        VALUES ({new_id}, {property_id}, {expense.amount}, '{expense.date}', '{expense.category}', {vendor}, {description})
    """
    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    return {"expense_id": new_id, "property_id": property_id, "amount": expense.amount, "date": str(expense.date), "category": expense.category, "vendor": expense.vendor, "description": expense.description}

# ---------------------------------------------------------------------------
# Custom Endpoints
# ---------------------------------------------------------------------------

# 1. GET /properties/{property_id}/summary — income vs expenses summary
@app.get("/properties/{property_id}/summary")
def get_property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not check:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    income_q = f"SELECT IFNULL(SUM(amount), 0) as total FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}"
    expense_q = f"SELECT IFNULL(SUM(amount), 0) as total FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}"
    total_income = list(bq.query(income_q).result())[0]["total"]
    total_expenses = list(bq.query(expense_q).result())[0]["total"]
    return {
        "property_id": property_id,
        "total_income": total_income,
        "total_expenses": total_expenses,
        "net": total_income - total_expenses
    }

# 2. GET /summary — portfolio-wide summary across all properties
@app.get("/summary")
def get_portfolio_summary(bq: bigquery.Client = Depends(get_bq_client)):
    income_q = f"SELECT IFNULL(SUM(amount), 0) as total FROM `{PROJECT_ID}.{DATASET}.income`"
    expense_q = f"SELECT IFNULL(SUM(amount), 0) as total FROM `{PROJECT_ID}.{DATASET}.expenses`"
    total_income = list(bq.query(income_q).result())[0]["total"]
    total_expenses = list(bq.query(expense_q).result())[0]["total"]
    return {
        "total_income": total_income,
        "total_expenses": total_expenses,
        "net": total_income - total_expenses
    }

# 3. GET /expenses/{property_id}/by-category — expenses grouped by category
@app.get("/expenses/{property_id}/by-category")
def get_expenses_by_category(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not check:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    query = f"""
        SELECT category, SUM(amount) as total
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = {property_id}
        GROUP BY category
        ORDER BY total DESC
    """
    results = bq.query(query).result()
    return [dict(row) for row in results]

# 4. GET /properties/{property_id}/vacant — check if property is vacant
@app.get("/properties/{property_id}/vacant")
def check_vacancy(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    results = list(bq.query(f"SELECT tenant_name FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not results:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    tenant = results[0]["tenant_name"]
    return {
        "property_id": property_id,
        "is_vacant": tenant is None or tenant == "",
        "tenant_name": tenant
    }

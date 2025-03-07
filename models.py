# models.py
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class InventoryItem(BaseModel):
    Supplier_Name: str
    Inventory_Item_Name: str
    Brand: str
    Item_Name: str
    Item_Number: str
    Category: str
    Case_Price: float
    Cost_of_a_Unit: float
    Quantity_In_a_Case: float
    Total_Units: float
    Priced_By: str

class UserInventory(BaseModel):
    id: str
    userId: str
    supplier_name: str
    items: List[Dict[str, Any]]
    timestamp: str
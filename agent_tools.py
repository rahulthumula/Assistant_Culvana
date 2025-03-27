# agent_tools.py
import logging
import json
from azure.cosmos import CosmosClient, PartitionKey
from config import (
    COSMOS_ENDPOINT,
    COSMOS_KEY,
    COSMOS_DATABASE,
    COSMOS_CONTAINER
)
from database import CosmosDB
from event_tracking import ItemEventTracker  # Import event tracker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AgentTools")

class InventoryAgent:
    """
    Agent class that provides tools to update and manage inventory items in CosmosDB
    """
    def __init__(self, user_id):
        self.user_id = user_id
        self.cosmos_db = CosmosDB()
        self.event_tracker = ItemEventTracker()  # Initialize event tracker
        logger.info(f"Initialized InventoryAgent for user {user_id}")
    
    async def _find_item_by_name_or_number(self, identifier):
        """
        Helper method to find an inventory item by either name or number
        
        Args:
            identifier (str): The item name or number to search for
            
        Returns:
            tuple: (found_item, item_doc, is_by_name) - the item, its document, and whether it was found by name
        """
        try:
            if not identifier:
                logger.error("Empty identifier provided to _find_item_by_name_or_number")
                return None, None, False
                
            logger.info(f"Searching for item with identifier: '{identifier}'")
            
            # First try to find by item number (exact match)
            query_by_number = f"SELECT * FROM c WHERE c.userId = '{self.user_id}'"
            logger.debug(f"Query by number: {query_by_number}")
            
            items_by_number = list(self.cosmos_db.container.query_items(
                query=query_by_number,
                enable_cross_partition_query=True
            ))
            
            if items_by_number:
                item_doc = items_by_number[0]
                logger.info(f"Found document: {item_doc.get('id')}")
                
                # Find the specific item with matching number
                for item in item_doc.get('items', []):
                    if item.get('Item Number') == identifier:
                        logger.info(f"Found item by number: {item.get('Inventory Item Name')} (#{item.get('Item Number')})")
                        return item, item_doc, False
            
                # Try to find by exact name match (case-insensitive)
                for item in item_doc.get('items', []):
                    item_name = item.get('Inventory Item Name', '')
                    if item_name.lower() == identifier.lower():
                        logger.info(f"Found exact name match: {item_name} (#{item.get('Item Number', 'unknown')})")
                        return item, item_doc, True
                
                # Try partial name match if exact match not found
                logger.info("No exact name match, trying partial match")
                matched_items = []
                for item in item_doc.get('items', []):
                    item_name = item.get('Inventory Item Name', '')
                    if identifier.lower() in item_name.lower():
                        matched_items.append((item, item_name))
                        logger.info(f"Found partial match: {item_name} (#{item.get('Item Number', 'unknown')})")
                
                # If we found multiple matches, use the closest one
                if matched_items:
                    # Sort by length of name (shorter = better match)
                    matched_items.sort(key=lambda x: len(x[1]))
                    best_match, best_name = matched_items[0]
                    logger.info(f"Selected best partial match: {best_name} (#{best_match.get('Item Number', 'unknown')})")
                    return best_match, item_doc, True
            
            # Not found by either method
            logger.warning(f"No matches found for identifier: '{identifier}'")
            return None, None, False
                
        except Exception as e:
            logger.error(f"Error finding item: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None, None, False

    async def update_item_price(self, item_identifier, new_price, price_type="Cost of a Unit"):
        """
        Update the price of an inventory item
        
        Args:
            item_identifier (str): The item name or number of the inventory item
            new_price (float): The new price to set
            price_type (str): The type of price to update (e.g., "Cost of a Unit", "Case Price")
            
        Returns:
            dict: Result of the operation
        """
        try:
            logger.info(f"Updating price for '{item_identifier}' to {new_price} ({price_type})")
            
            # Validate inputs
            if not item_identifier:
                logger.error("Empty item identifier provided")
                return {
                    "success": False,
                    "message": "Item identifier cannot be empty"
                }
                
            try:
                # Make sure new_price is a float
                new_price = float(new_price)
            except (ValueError, TypeError):
                logger.error(f"Invalid price value: {new_price}")
                return {
                    "success": False,
                    "message": f"Invalid price: {new_price}. Price must be a number."
                }
                
            # Validate price type
            if price_type not in ["Cost of a Unit", "Case Price"]:
                logger.warning(f"Invalid price type: {price_type}, defaulting to 'Cost of a Unit'")
                price_type = "Cost of a Unit"
            
            # Find the item by name or number
            found_item, item_doc, is_by_name = await self._find_item_by_name_or_number(item_identifier)
            
            if not found_item or not item_doc:
                identifier_type = "name" if is_by_name else "number"
                logger.warning(f"No item found with {identifier_type} '{item_identifier}' for user {self.user_id}")
                return {
                    "success": False,
                    "message": f"Item with {identifier_type} '{item_identifier}' not found in inventory"
                }
            
            # Update the price in the item
            item_name = found_item.get('Inventory Item Name')
            item_number = found_item.get('Item Number')
            supplier_name = found_item.get('Supplier Name', '')
            
            logger.info(f"Found item: {item_name} (#{item_number})")
            
            changes = {}  # Track changes for event logging
            
            if price_type in found_item:
                old_price = found_item[price_type]
                found_item[price_type] = float(new_price)
                logger.info(f"Updated {price_type} from {old_price} to {new_price} for item {item_name} (#{item_number})")
                
                # Record the change
                changes[price_type] = {
                    "old": old_price,
                    "new": float(new_price)
                }
                
                # If updating unit cost, recalculate case price if possible
                if price_type == "Cost of a Unit" and "Quantity In a Case" in found_item:
                    qty_in_case = float(found_item["Quantity In a Case"])
                    if qty_in_case > 0:
                        old_case_price = found_item.get("Case Price", old_price * qty_in_case)
                        found_item["Case Price"] = float(new_price) * qty_in_case
                        logger.info(f"Recalculated Case Price to {found_item['Case Price']}")
                        
                        # Record the change
                        changes["Case Price"] = {
                            "old": old_case_price,
                            "new": found_item["Case Price"]
                        }
                
                # If updating case price, recalculate unit cost if possible
                elif price_type == "Case Price" and "Quantity In a Case" in found_item:
                    qty_in_case = float(found_item["Quantity In a Case"])
                    if qty_in_case > 0:
                        old_unit_cost = found_item.get("Cost of a Unit", old_price / qty_in_case)
                        found_item["Cost of a Unit"] = float(new_price) / qty_in_case
                        logger.info(f"Recalculated Cost of a Unit to {found_item['Cost of a Unit']}")
                        
                        # Record the change
                        changes["Cost of a Unit"] = {
                            "old": old_unit_cost,
                            "new": found_item["Cost of a Unit"]
                        }
            else:
                logger.warning(f"Price field '{price_type}' not found in item {item_name}")
                return {
                    "success": False,
                    "message": f"Price field '{price_type}' not found in item {item_name}"
                }
            
            # Replace the document in CosmosDB
            logger.info(f"Updating document in CosmosDB with ID: {item_doc['id']}")
            try:
                self.cosmos_db.container.replace_item(
                    item=item_doc['id'],
                    body=item_doc
                )
                logger.info("Successfully updated document in CosmosDB")
                
                # Log the price change event
                self.event_tracker.track_item_updated(
                    user_id=self.user_id,
                    item_number=item_number,
                    changes=changes,
                    supplier_name=supplier_name
                )
                
            except Exception as db_error:
                logger.error(f"Error updating document in CosmosDB: {str(db_error)}")
                import traceback
                logger.error(traceback.format_exc())
                return {
                    "success": False,
                    "message": f"Database error: {str(db_error)}"
                }
            
            return {
                "success": True,
                "message": f"Successfully updated {price_type} to ${new_price} for {item_name} (#{item_number})"
            }
            
        except Exception as e:
            logger.error(f"Error updating item price: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error updating price: {str(e)}"
            }
    
    async def update_item_quantity(self, item_identifier, new_quantity):
        """
        Update the quantity of an inventory item
        
        Args:
            item_identifier (str): The item name or number of the inventory item
            new_quantity (float): The new quantity to set
            
        Returns:
            dict: Result of the operation
        """
        try:
            logger.info(f"Updating quantity for '{item_identifier}' to {new_quantity}")
            
            # Validate inputs
            if not item_identifier:
                logger.error("Empty item identifier provided")
                return {
                    "success": False,
                    "message": "Item identifier cannot be empty"
                }
                
            try:
                # Make sure new_quantity is a float
                new_quantity = float(new_quantity)
            except (ValueError, TypeError):
                logger.error(f"Invalid quantity value: {new_quantity}")
                return {
                    "success": False,
                    "message": f"Invalid quantity: {new_quantity}. Quantity must be a number."
                }
            
            # Find the item by name or number
            found_item, item_doc, is_by_name = await self._find_item_by_name_or_number(item_identifier)
            
            if not found_item or not item_doc:
                identifier_type = "name" if is_by_name else "number"
                logger.warning(f"No item found with {identifier_type} '{item_identifier}' for user {self.user_id}")
                return {
                    "success": False,
                    "message": f"Item with {identifier_type} '{item_identifier}' not found in inventory"
                }
            
            # Update the quantity in the item
            item_name = found_item.get('Inventory Item Name')
            item_number = found_item.get('Item Number')
            supplier_name = found_item.get('Supplier Name', '')
            
            logger.info(f"Found item: {item_name} (#{item_number})")
            
            changes = {}  # Track changes for event logging
            
            if "Total Units" in found_item:
                old_qty = found_item["Total Units"]
                found_item["Total Units"] = float(new_quantity)
                logger.info(f"Updated Total Units from {old_qty} to {new_quantity} for item {item_name} (#{item_number})")
                
                # Record the change
                changes["Total Units"] = {
                    "old": old_qty,
                    "new": float(new_quantity)
                }
            else:
                logger.warning(f"Total Units field not found in item {item_name}")
                return {
                    "success": False,
                    "message": f"Total Units field not found in item {item_name}"
                }
            
            # Replace the document in CosmosDB
            logger.info(f"Updating document in CosmosDB with ID: {item_doc['id']}")
            try:
                self.cosmos_db.container.replace_item(
                    item=item_doc['id'],
                    body=item_doc
                )
                logger.info("Successfully updated document in CosmosDB")
                
                # Log the quantity change event
                self.event_tracker.track_item_updated(
                    user_id=self.user_id,
                    item_number=item_number,
                    changes=changes,
                    supplier_name=supplier_name
                )
                
            except Exception as db_error:
                logger.error(f"Error updating document in CosmosDB: {str(db_error)}")
                import traceback
                logger.error(traceback.format_exc())
                return {
                    "success": False,
                    "message": f"Database error: {str(db_error)}"
                }
            
            return {
                "success": True,
                "message": f"Successfully updated Total Units to {new_quantity} for {item_name} (#{item_number})"
            }
            
        except Exception as e:
            logger.error(f"Error updating item quantity: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error updating quantity: {str(e)}"
            }
    
    async def add_new_inventory_item(self, item_data):
        """
        Add a new inventory item
        
        Args:
            item_data (dict): The data for the new inventory item
            
        Returns:
            dict: Result of the operation
        """
        try:
            logger.info(f"Adding new inventory item: {item_data.get('Inventory Item Name', 'Unknown')}")
            
            # Validate required fields
            required_fields = ["Inventory Item Name", "Category", "Cost of a Unit"]
            for field in required_fields:
                if field not in item_data:
                    logger.error(f"Missing required field: {field}")
                    return {
                        "success": False,
                        "message": f"Missing required field: {field}"
                    }
            
            # Auto-generate item number if not provided
            if "Item Number" not in item_data:
                # Generate based on first letters of item name and random numbers
                import uuid
                name_prefix = ''.join([word[0].upper() for word in item_data["Inventory Item Name"].split()[:3]])
                random_suffix = str(uuid.uuid4())[:6].upper()
                item_data["Item Number"] = f"{name_prefix}{random_suffix}"
                logger.info(f"Generated item number {item_data['Item Number']} for {item_data['Inventory Item Name']}")
            
            # Query to find the user's document
            query = f"SELECT * FROM c WHERE c.userId = '{self.user_id}'"
            
            items = list(self.cosmos_db.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            
            if not items:
                logger.warning(f"No inventory document found for user {self.user_id}")
                return {
                    "success": False,
                    "message": f"No inventory document found for user {self.user_id}"
                }
            
            # Get the first document (should be the user's inventory document)
            inventory_doc = items[0]
            
            # Check if item already exists by name
            for existing_item in inventory_doc.get('items', []):
                if existing_item.get('Inventory Item Name', '').lower() == item_data['Inventory Item Name'].lower():
                    logger.warning(f"Item with name '{item_data['Inventory Item Name']}' already exists")
                    return {
                        "success": False,
                        "message": f"Item with name '{item_data['Inventory Item Name']}' already exists"
                    }
            
            # Add the new item
            inventory_doc['items'].append(item_data)
            
            # Replace the document in CosmosDB
            logger.info(f"Updating document in CosmosDB with ID: {inventory_doc['id']}")
            try:
                self.cosmos_db.container.replace_item(
                    item=inventory_doc['id'],
                    body=inventory_doc
                )
                logger.info("Successfully updated document in CosmosDB")
                
                # Log the item creation event
                self.event_tracker.track_item_created(
                    user_id=self.user_id,
                    item_data=item_data
                )
                
            except Exception as db_error:
                logger.error(f"Error updating document in CosmosDB: {str(db_error)}")
                import traceback
                logger.error(traceback.format_exc())
                return {
                    "success": False,
                    "message": f"Database error: {str(db_error)}"
                }
            
            return {
                "success": True,
                "message": f"Successfully added new item '{item_data['Inventory Item Name']}' with item number #{item_data['Item Number']}"
            }
            
        except Exception as e:
            logger.error(f"Error adding new inventory item: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error adding new item: {str(e)}"
            }
    
    async def delete_inventory_item(self, item_identifier):
        """
        Delete an inventory item
        
        Args:
            item_identifier (str): The item name or number of the inventory item to delete
            
        Returns:
            dict: Result of the operation
        """
        try:
            logger.info(f"Deleting inventory item: {item_identifier}")
            
            # Validate inputs
            if not item_identifier:
                logger.error("Empty item identifier provided")
                return {
                    "success": False,
                    "message": "Item identifier cannot be empty"
                }
            
            # Find the item by name or number
            found_item, item_doc, is_by_name = await self._find_item_by_name_or_number(item_identifier)
            
            if not found_item or not item_doc:
                identifier_type = "name" if is_by_name else "number"
                logger.warning(f"No item found with {identifier_type} '{item_identifier}' for user {self.user_id}")
                return {
                    "success": False,
                    "message": f"Item with {identifier_type} '{item_identifier}' not found in inventory"
                }
            
            # Get item details
            item_name = found_item.get('Inventory Item Name')
            item_number = found_item.get('Item Number')
            
            logger.info(f"Found item to delete: {item_name} (#{item_number})")
            
            # Find the item index
            item_index = None
            for i, item in enumerate(item_doc.get('items', [])):
                if (item.get('Item Number') == item_number or 
                    item.get('Inventory Item Name', '').lower() == item_name.lower()):
                    item_index = i
                    break
            
            if item_index is None:
                logger.warning(f"Could not locate item '{item_name}' in the document")
                return {
                    "success": False,
                    "message": f"Could not locate item '{item_name}' in the document"
                }
            
            # Create a copy of the item before removing it
            item_data = found_item.copy()
            
            # Remove the item
            deleted_item = item_doc['items'].pop(item_index)
            logger.info(f"Removed item from array at index {item_index}")
            
            # Replace the document in CosmosDB
            logger.info(f"Updating document in CosmosDB with ID: {item_doc['id']}")
            try:
                self.cosmos_db.container.replace_item(
                    item=item_doc['id'],
                    body=item_doc
                )
                logger.info("Successfully updated document in CosmosDB")
                
                # Log the item deletion event
                self.event_tracker.track_item_deleted(
                    user_id=self.user_id,
                    item_number=item_number,
                    item_data=item_data
                )
                
            except Exception as db_error:
                logger.error(f"Error updating document in CosmosDB: {str(db_error)}")
                import traceback
                logger.error(traceback.format_exc())
                return {
                    "success": False,
                    "message": f"Database error: {str(db_error)}"
                }
            
            return {
                "success": True,
                "message": f"Successfully deleted item '{item_name}' with number #{item_number}"
            }
            
        except Exception as e:
            logger.error(f"Error deleting inventory item: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error deleting item: {str(e)}"
            }

    async def search_items_by_category(self, category):
        """
        Search for items by category
        
        Args:
            category (str): The category to search for
            
        Returns:
            dict: Result of the operation with matching items
        """
        try:
            logger.info(f"Searching for items in category: {category}")
            
            # Validate inputs
            if not category:
                logger.error("Empty category provided")
                return {
                    "success": False,
                    "message": "Category cannot be empty",
                    "items": []
                }
            
            # Query to find documents for specific user
            query = f"SELECT * FROM c WHERE c.userId = '{self.user_id}'"
            
            items = list(self.cosmos_db.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            
            if not items:
                logger.warning(f"No inventory document found for user {self.user_id}")
                return {
                    "success": False,
                    "message": f"No inventory document found for user {self.user_id}",
                    "items": []
                }
            
            # Get the first document (should be the user's inventory document)
            inventory_doc = items[0]
            
            # Filter items by category (case-insensitive)
            matching_items = []
            for item in inventory_doc.get('items', []):
                if item.get('Category', '').lower() == category.lower():
                    matching_items.append(item)
            
            logger.info(f"Found {len(matching_items)} items in category '{category}'")
            
            return {
                "success": True,
                "message": f"Found {len(matching_items)} items in category '{category}'",
                "items": matching_items
            }
            
        except Exception as e:
            logger.error(f"Error searching items by category: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error searching items: {str(e)}",
                "items": []
            }
    
    async def get_item_details(self, item_identifier):
        """
        Get detailed information about a specific inventory item
        
        Args:
            item_identifier (str): The item name or number of the inventory item
            
        Returns:
            dict: Result of the operation with item details
        """
        try:
            logger.info(f"Getting details for item: {item_identifier}")
            
            # Validate inputs
            if not item_identifier:
                logger.error("Empty item identifier provided")
                return {
                    "success": False,
                    "message": "Item identifier cannot be empty",
                    "item": None
                }
            
            # Find the item by name or number
            found_item, item_doc, is_by_name = await self._find_item_by_name_or_number(item_identifier)
            
            if not found_item or not item_doc:
                identifier_type = "name" if is_by_name else "number"
                logger.warning(f"No item found with {identifier_type} '{item_identifier}' for user {self.user_id}")
                return {
                    "success": False,
                    "message": f"Item with {identifier_type} '{item_identifier}' not found in inventory",
                    "item": None
                }
            
            item_name = found_item.get('Inventory Item Name')
            logger.info(f"Found item details for: {item_name}")
            
            return {
                "success": True,
                "message": f"Found item '{item_name}'",
                "item": found_item
            }
            
        except Exception as e:
            logger.error(f"Error getting item details: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error retrieving item details: {str(e)}",
                "item": None
            }
    
    async def get_item_history(self, item_identifier):
        """
        Get the complete change history for an item
        
        Args:
            item_identifier (str): The item name or number of the inventory item
            
        Returns:
            dict: Result of the operation with item history
        """
        try:
            logger.info(f"Getting history for item: {item_identifier}")
            
            # Validate inputs
            if not item_identifier:
                logger.error("Empty item identifier provided")
                return {
                    "success": False,
                    "message": "Item identifier cannot be empty",
                    "history": []
                }
            
            # Find the item by name or number to get the item number
            found_item, item_doc, is_by_name = await self._find_item_by_name_or_number(item_identifier)
            
            if not found_item or not item_doc:
                identifier_type = "name" if is_by_name else "number"
                logger.warning(f"No item found with {identifier_type} '{item_identifier}' for user {self.user_id}")
                return {
                    "success": False,
                    "message": f"Item with {identifier_type} '{item_identifier}' not found in inventory",
                    "history": []
                }
            
            # Get the item number
            item_name = found_item.get('Inventory Item Name')
            item_number = found_item.get('Item Number')
            
            logger.info(f"Found item: {item_name} (#{item_number})")
            
            # Get the item history from event tracker
            history = self.event_tracker.get_item_history(item_number)
            
            return {
                "success": True,
                "message": f"Retrieved {len(history)} historical events for item '{item_name}' (#{item_number})",
                "item_name": item_name,
                "item_number": item_number,
                "history": history
            }
            
        except Exception as e:
            logger.error(f"Error getting item history: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error retrieving item history: {str(e)}",
                "history": []
            }
    
    async def get_recent_changes(self, limit=10, event_types=None):
        """
        Get recent changes across all inventory items
        
        Args:
            limit (int): Maximum number of events to return
            event_types (list): Optional filter by event types
            
        Returns:
            dict: Result of the operation with recent changes
        """
        try:
            logger.info(f"Getting recent changes for user {self.user_id}")
            
            # Get recent events from event tracker
            events = await self.event_tracker.get_recent_events(
                user_id=self.user_id,
                limit=limit,
                event_types=event_types
            )
            
            return {
                "success": True,
                "message": f"Retrieved {len(events)} recent events",
                "events": events
            }
            
        except Exception as e:
            logger.error(f"Error getting recent changes: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error retrieving recent changes: {str(e)}",
                "events": []
            }
    
    async def create_item_snapshot(self, item_identifier, changed_by=None):
        """
        Create a point-in-time snapshot of an item
        
        Args:
            item_identifier (str): The item name or number of the inventory item
            changed_by (str): Optional - who performed the action
            
        Returns:
            dict: Result of the operation
        """
        try:
            logger.info(f"Creating snapshot for item: {item_identifier}")
            
            # Validate inputs
            if not item_identifier:
                logger.error("Empty item identifier provided")
                return {
                    "success": False,
                    "message": "Item identifier cannot be empty"
                }
            
            # Find the item by name or number
            found_item, item_doc, is_by_name = await self._find_item_by_name_or_number(item_identifier)
            
            if not found_item or not item_doc:
                identifier_type = "name" if is_by_name else "number"
                logger.warning(f"No item found with {identifier_type} '{item_identifier}' for user {self.user_id}")
                return {
                    "success": False,
                    "message": f"Item with {identifier_type} '{item_identifier}' not found in inventory"
                }
            
            # Get item details
            item_name = found_item.get('Inventory Item Name')
            item_number = found_item.get('Item Number')
            
            logger.info(f"Found item: {item_name} (#{item_number})")
            
            # Create snapshot
            result = self.event_tracker.create_snapshot(
                user_id=self.user_id,
                item_number=item_number,
                item_data=found_item,
                changed_by=changed_by
            )
            
            if result:
                return {
                    "success": True,
                    "message": f"Successfully created snapshot for item '{item_name}' (#{item_number})"
                }
            else:
                return {
                    "success": False,
                    "message": f"Failed to create snapshot for item '{item_name}' (#{item_number})"
                }
            
        except Exception as e:
            logger.error(f"Error creating item snapshot: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error creating snapshot: {str(e)}"
            }
    
    async def get_price_change_history(self, item_identifier=None, days=30):
        """
        Get price change history for all items or a specific item
        
        Args:
            item_identifier (str): Optional item name or number to filter by
            days (int): Number of days to look back
            
        Returns:
            dict: Result of the operation with price history
        """
        try:
            logger.info(f"Getting price history for user {self.user_id}")
            
            item_number = None
            item_name = None
            
            # If item_identifier provided, find the item to get its number
            if item_identifier:
                found_item, item_doc, is_by_name = await self._find_item_by_name_or_number(item_identifier)
                
                if not found_item or not item_doc:
                    identifier_type = "name" if is_by_name else "number"
                    logger.warning(f"No item found with {identifier_type} '{item_identifier}' for user {self.user_id}")
                    return {
                        "success": False,
                        "message": f"Item with {identifier_type} '{item_identifier}' not found in inventory",
                        "price_history": []
                    }
                
                item_name = found_item.get('Inventory Item Name')
                item_number = found_item.get('Item Number')
                
                logger.info(f"Found item: {item_name} (#{item_number})")
            
            # Get price history from event tracker
            history = await self.event_tracker.get_price_change_history(
                user_id=self.user_id,
                item_number=item_number,
                days=days
            )
            
            message = f"Retrieved {len(history)} price change events"
            if item_name:
                message += f" for item '{item_name}' (#{item_number})"
            message += f" in the last {days} days"
            
            return {
                "success": True,
                "message": message,
                "price_history": history
            }
            
        except Exception as e:
            logger.error(f"Error getting price change history: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error retrieving price history: {str(e)}",
                "price_history": []
            }
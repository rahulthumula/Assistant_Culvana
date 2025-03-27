# event_tracking.py
import logging
import json
from datetime import datetime
from azure.cosmos import CosmosClient, PartitionKey
from config import (
    COSMOS_ENDPOINT,
    COSMOS_KEY,
    COSMOS_DATABASE
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EventTracking")

class ItemEventTracker:
    """
    Handles tracking and storing of inventory item events in a dedicated container
    """
    def __init__(self):
        self.client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
        self.database = self.client.get_database_client(COSMOS_DATABASE)
        
        # Use a dedicated container for events
        self.container_name = "inventory_historicaldata"
        
        # Create container if it doesn't exist
        self._ensure_container_exists()
        
        self.container = self.database.get_container_client(self.container_name)
        logger.info(f"Initialized ItemEventTracker with container: {self.container_name}")
    
    def _ensure_container_exists(self):
        """Create the events container if it doesn't exist"""
        container_list = [container['id'] for container in self.database.list_containers()]
        
        if self.container_name not in container_list:
            logger.info(f"Creating new events container: {self.container_name}")
            self.database.create_container(
                id=self.container_name,
                partition_key=PartitionKey(path="/item_number"),
                default_ttl=None  # No automatic deletion - events are permanent
            )
            logger.info(f"Successfully created container: {self.container_name}")
    
    def _generate_event_id(self, item_number):
        """Generate a unique ID for an event"""
        timestamp = datetime.utcnow().isoformat()
        return f"{item_number}-{timestamp}"
    
    def track_item_created(self, user_id, item_data, batch_number=1, changed_by=None):
        """
        Track item creation event
        
        Args:
            user_id (str): User who owns the item
            item_data (dict): Full item data
            batch_number (int): Optional batch/version number
            changed_by (str): Optional - who performed the action
        """
        try:
            item_number = item_data.get('Item Number')
            if not item_number:
                logger.error("Cannot track item creation: Missing Item Number")
                return None
            
            # Build event document
            event = {
                "id": self._generate_event_id(item_number),
                "eventType": "ITEM_CREATED",
                "item_number": item_number,
                "userId": user_id,
                "supplier_name": item_data.get('Supplier Name', ''),
                "timestamp": datetime.utcnow().isoformat(),
                "batchNumber": batch_number
            }
            
            # Add changed_by if provided
            if changed_by:
                event["changed_by"] = changed_by
            
            # Add full item snapshot
            event["snapshot"] = {k: v for k, v in item_data.items()}
            
            # Store event
            result = self.container.create_item(body=event)
            logger.info(f"Created ITEM_CREATED event for item {item_number}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error tracking item creation: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def track_item_updated(self, user_id, item_number, changes, snapshot=None, changed_by=None, supplier_name=None):
        """
        Track item update event
        
        Args:
            user_id (str): User who owns the item
            item_number (str): Item number/identifier
            changes (dict): Field changes in format {field: {"old": old_value, "new": new_value}}
            snapshot (dict): Optional full item snapshot after update
            changed_by (str): Optional - who performed the action
            supplier_name (str): Optional supplier name
        """
        try:
            if not item_number:
                logger.error("Cannot track item update: Missing Item Number")
                return None
            
            # Determine specific event type based on fields changed
            event_type = "ITEM_UPDATED"
            if len(changes) == 1:
                if "Cost of a Unit" in changes or "Case Price" in changes:
                    event_type = "PRICE_UPDATED"
                elif "Total Units" in changes:
                    event_type = "QUANTITY_UPDATED"
                elif "Measured In" in changes or "Quantity In a Case" in changes:
                    event_type = "UNIT_UPDATED"
            
            # Build event document
            event = {
                "id": self._generate_event_id(item_number),
                "eventType": event_type,
                "item_number": item_number,
                "userId": user_id,
                "timestamp": datetime.utcnow().isoformat(),
                "changes": changes
            }
            
            # Add supplier_name if provided
            if supplier_name:
                event["supplier_name"] = supplier_name
                
            # Add changed_by if provided
            if changed_by:
                event["changed_by"] = changed_by
            
            # Add full item snapshot if provided
            if snapshot:
                event["snapshot"] = snapshot
            
            # Store event
            result = self.container.create_item(body=event)
            logger.info(f"Created {event_type} event for item {item_number}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error tracking item update: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def track_item_deleted(self, user_id, item_number, item_data=None, changed_by=None):
        """
        Track item deletion event
        
        Args:
            user_id (str): User who owns the item
            item_number (str): Item number/identifier
            item_data (dict): Optional full item data prior to deletion
            changed_by (str): Optional - who performed the action
        """
        try:
            if not item_number:
                logger.error("Cannot track item deletion: Missing Item Number")
                return None
            
            # Build event document
            event = {
                "id": self._generate_event_id(item_number),
                "eventType": "ITEM_DELETED",
                "item_number": item_number,
                "userId": user_id,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Add changed_by if provided
            if changed_by:
                event["changed_by"] = changed_by
            
            # Add supplier name if available
            if item_data and "Supplier Name" in item_data:
                event["supplier_name"] = item_data["Supplier Name"]
            
            # Add full item snapshot if provided
            if item_data:
                event["snapshot"] = {k: v for k, v in item_data.items()}
            
            # Store event
            result = self.container.create_item(body=event)
            logger.info(f"Created ITEM_DELETED event for item {item_number}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error tracking item deletion: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def get_item_history(self, item_number, limit=None, event_types=None):
        """
        Get the complete history for an item
        
        Args:
            item_number (str): Item number/identifier
            limit (int): Optional limit on number of events
            event_types (list): Optional filter by event types
            
        Returns:
            list: Event history in chronological order
        """
        try:
            if not item_number:
                logger.error("Cannot retrieve item history: Missing Item Number")
                return []
            
            # Base query
            query = f"SELECT * FROM c WHERE c.item_number = '{item_number}'"
            
            # Add event type filter if specified
            if event_types and isinstance(event_types, list) and len(event_types) > 0:
                event_types_str = "', '".join(event_types)
                query += f" AND c.eventType IN ('{event_types_str}')"
            
            # Add ORDER BY clause
            query += " ORDER BY c.timestamp ASC"
            
            # Execute query
            items = list(self.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            
            # Apply limit if specified
            if limit and isinstance(limit, int):
                items = items[:limit]
            
            logger.info(f"Retrieved {len(items)} historical events for item {item_number}")
            return items
            
        except Exception as e:
            logger.error(f"Error retrieving item history: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def create_snapshot(self, user_id, item_number, item_data, changed_by=None):
        """
        Create a point-in-time snapshot of the item
        
        Args:
            user_id (str): User who owns the item
            item_number (str): Item number/identifier
            item_data (dict): Full item data
            changed_by (str): Optional - who performed the action
        """
        try:
            if not item_number:
                logger.error("Cannot create snapshot: Missing Item Number")
                return None
            
            # Build event document
            event = {
                "id": f"{item_number}-snapshot-{datetime.utcnow().isoformat()}",
                "eventType": "ITEM_SNAPSHOT",
                "item_number": item_number,
                "userId": user_id,
                "timestamp": datetime.utcnow().isoformat(),
                "supplier_name": item_data.get('Supplier Name', '')
            }
            
            # Add changed_by if provided
            if changed_by:
                event["changed_by"] = changed_by
            
            # Add full item snapshot
            event["snapshot"] = {k: v for k, v in item_data.items()}
            
            # Store event
            result = self.container.create_item(body=event)
            logger.info(f"Created ITEM_SNAPSHOT event for item {item_number}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating item snapshot: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    async def get_recent_events(self, user_id, limit=10, event_types=None):
        """
        Get recent events across all items for a user
        
        Args:
            user_id (str): User ID
            limit (int): Maximum number of events to return
            event_types (list): Optional filter by event types
            
        Returns:
            list: Recent events in reverse chronological order
        """
        try:
            # Base query
            query = f"SELECT * FROM c WHERE c.userId = '{user_id}'"
            
            # Add event type filter if specified
            if event_types and isinstance(event_types, list) and len(event_types) > 0:
                event_types_str = "', '".join(event_types)
                query += f" AND c.eventType IN ('{event_types_str}')"
            
            # Add ORDER BY clause for reverse chronological
            query += " ORDER BY c.timestamp DESC"
            
            # Execute query
            items = list(self.container.query_items(
                query=query,
                enable_cross_partition_query=True,
                max_item_count=limit
            ))
            
            logger.info(f"Retrieved {len(items)} recent events for user {user_id}")
            return items
            
        except Exception as e:
            logger.error(f"Error retrieving recent events: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    async def get_price_change_history(self, user_id, item_number=None, days=30):
        """
        Get price change history for all items or a specific item
        
        Args:
            user_id (str): User ID
            item_number (str): Optional item number to filter by
            days (int): Number of days to look back
            
        Returns:
            list: Price change events
        """
        try:
            # Calculate date threshold
            from datetime import datetime, timedelta
            threshold_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
            
            # Base query
            query = (f"SELECT * FROM c WHERE c.userId = '{user_id}' "
                    f"AND c.eventType = 'PRICE_UPDATED' "
                    f"AND c.timestamp >= '{threshold_date}'")
            
            # Add item filter if specified
            if item_number:
                query += f" AND c.item_number = '{item_number}'"
            
            # Add ORDER BY clause
            query += " ORDER BY c.timestamp DESC"
            
            # Execute query
            items = list(self.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            
            logger.info(f"Retrieved {len(items)} price change events")
            return items
            
        except Exception as e:
            logger.error(f"Error retrieving price change history: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
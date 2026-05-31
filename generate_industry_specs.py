# generate_industry_specs.py
# ============================================================
# DEPRECATED — One-off script used to generate sample tracking
# plan JSON files during early development.
# Not imported anywhere. Kept for reference only.
# ============================================================
import json
import os

SEGMENT_ECOMMERCE_SPEC = {
    "events": [
        {
            "name": "Products Searched",
            "required_properties": ["query"],
            "property_types": {"query": "string"},
            "identity_required": False,
            "allowed_previous_events": []
        },
        {
            "name": "Product List Viewed",
            "required_properties": ["list_id", "category"],
            "property_types": {"list_id": "string", "category": "string"},
            "identity_required": False,
            "allowed_previous_events": []
        },
        {
            "name": "Product List Filtered",
            "required_properties": ["list_id", "filters"],
            "property_types": {"list_id": "string", "filters": "array"},
            "identity_required": False,
            "allowed_previous_events": ["Product List Viewed"]
        },
        {
            "name": "Promotion Viewed",
            "required_properties": ["promotion_id", "name"],
            "property_types": {"promotion_id": "string", "name": "string"},
            "identity_required": False,
            "allowed_previous_events": []
        },
        {
            "name": "Promotion Clicked",
            "required_properties": ["promotion_id", "name"],
            "property_types": {"promotion_id": "string", "name": "string"},
            "identity_required": False,
            "allowed_previous_events": ["Promotion Viewed"]
        },
        {
            "name": "Product Clicked",
            "required_properties": ["product_id", "name", "price"],
            "property_types": {"product_id": "string", "name": "string", "price": "number"},
            "identity_required": False,
            "allowed_previous_events": ["Product List Viewed"]
        },
        {
            "name": "Product Viewed",
            "required_properties": ["product_id", "name", "price"],
            "property_types": {"product_id": "string", "name": "string", "price": "number"},
            "identity_required": False,
            "allowed_previous_events": []
        },
        {
            "name": "Product Added",
            "required_properties": ["cart_id", "product_id", "name", "price", "quantity"],
            "property_types": {"cart_id": "string", "product_id": "string", "name": "string", "price": "number", "quantity": "number"},
            "identity_required": True,
            "allowed_previous_events": ["Product Viewed"]
        },
        {
            "name": "Product Removed",
            "required_properties": ["cart_id", "product_id", "name", "price", "quantity"],
            "property_types": {"cart_id": "string", "product_id": "string", "name": "string", "price": "number", "quantity": "number"},
            "identity_required": True,
            "allowed_previous_events": ["Product Added", "Cart Viewed"]
        },
        {
            "name": "Cart Viewed",
            "required_properties": ["cart_id", "products"],
            "property_types": {"cart_id": "string", "products": "array"},
            "identity_required": True,
            "allowed_previous_events": ["Product Added"]
        },
        {
            "name": "Checkout Started",
            "required_properties": ["order_id", "revenue", "currency"],
            "property_types": {"order_id": "string", "revenue": "number", "currency": "string"},
            "identity_required": True,
            "allowed_previous_events": ["Cart Viewed", "Product Added"]
        },
        {
            "name": "Checkout Step Viewed",
            "required_properties": ["checkout_id", "step"],
            "property_types": {"checkout_id": "string", "step": "number"},
            "identity_required": True,
            "allowed_previous_events": ["Checkout Started"]
        },
        {
            "name": "Checkout Step Completed",
            "required_properties": ["checkout_id", "step"],
            "property_types": {"checkout_id": "string", "step": "number"},
            "identity_required": True,
            "allowed_previous_events": ["Checkout Step Viewed"]
        },
        {
            "name": "Payment Info Entered",
            "required_properties": ["checkout_id", "step"],
            "property_types": {"checkout_id": "string", "step": "number"},
            "identity_required": True,
            "allowed_previous_events": ["Checkout Step Viewed", "Checkout Step Completed"]
        },
        {
            "name": "Order Completed",
            "required_properties": ["order_id", "revenue", "currency", "products"],
            "property_types": {"order_id": "string", "revenue": "number", "currency": "string", "products": "array"},
            "identity_required": True,
            "allowed_previous_events": ["Checkout Started", "Payment Info Entered"]
        },
        {
            "name": "Order Updated",
            "required_properties": ["order_id", "revenue", "currency"],
            "property_types": {"order_id": "string", "revenue": "number", "currency": "string"},
            "identity_required": True,
            "allowed_previous_events": ["Order Completed"]
        },
        {
            "name": "Order Refunded",
            "required_properties": ["order_id", "revenue", "currency"],
            "property_types": {"order_id": "string", "revenue": "number", "currency": "string"},
            "identity_required": True,
            "allowed_previous_events": ["Order Completed"]
        },
        {
            "name": "Order Cancelled",
            "required_properties": ["order_id"],
            "property_types": {"order_id": "string"},
            "identity_required": True,
            "allowed_previous_events": ["Order Completed"]
        }
    ]
}

SNOWPLOW_ECOMMERCE_SPEC = {
    "events": [
        {
            "name": "page_view",
            "required_properties": ["url", "referrer"],
            "property_types": {"url": "string", "referrer": "string"},
            "identity_required": False,
            "allowed_previous_events": []
        },
        {
            "name": "add_to_cart",
            "required_properties": ["sku", "name", "unitPrice", "quantity"],
            "property_types": {"sku": "string", "name": "string", "unitPrice": "number", "quantity": "number"},
            "identity_required": True,
            "allowed_previous_events": ["page_view"]
        },
        {
            "name": "remove_from_cart",
            "required_properties": ["sku", "name", "unitPrice", "quantity"],
            "property_types": {"sku": "string", "name": "string", "unitPrice": "number", "quantity": "number"},
            "identity_required": True,
            "allowed_previous_events": ["add_to_cart", "page_view"]
        },
        {
            "name": "transaction",
            "required_properties": ["orderId", "total", "currency", "tax", "shipping"],
            "property_types": {"orderId": "string", "total": "number", "currency": "string", "tax": "number", "shipping": "number"},
            "identity_required": True,
            "allowed_previous_events": ["add_to_cart"]
        },
        {
            "name": "transaction_item",
            "required_properties": ["orderId", "sku", "name", "price", "quantity"],
            "property_types": {"orderId": "string", "sku": "string", "name": "string", "price": "number", "quantity": "number"},
            "identity_required": True,
            "allowed_previous_events": ["transaction"]
        }
    ]
}

def main():
    os.makedirs("sample_data", exist_ok=True)
    
    with open("sample_data/tracking_plan_segment.json", "w") as f:
        json.dump(SEGMENT_ECOMMERCE_SPEC, f, indent=2)
        
    with open("sample_data/tracking_plan_snowplow.json", "w") as f:
        json.dump(SNOWPLOW_ECOMMERCE_SPEC, f, indent=2)

    print("Generated Industry Standard Specs in sample_data/")

if __name__ == "__main__":
    main()

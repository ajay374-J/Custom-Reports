// Copyright (c) 2025, Ajay and contributors
// For license information, please see license.txt

frappe.query_reports["Production Details Stock Wise For Month"] = {
	"filters": [
		{
            "fieldname": "from_date",
            "label": __("From Date"),
            "fieldtype": "Date",
			"reqd":1
        },
        {
            "fieldname": "to_date",
            "label": __("To Date"),
            "fieldtype": "Date",
			"reqd":1
        },
        {
            "fieldname": "item",
            "label": __("FG Item"),
            "fieldtype": "Link",
            "options":"Item"
        }
	]
};

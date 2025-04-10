# Copyright (c) 2025, Ajay and contributors
# For license information, please see license.txt

# import frappe


import frappe
from frappe.utils import flt


def execute(filters=None):
	columns=get_columns(filters)
	data=get_data(filters)
	return columns, data



def get_columns(filters):
	condition=""
	if filters.from_date and filters.to_date:
		condition+="and se.transaction_date>='{0}' and se.transaction_date<='{1}'".format(filters.from_date ,filters.to_date)
	
	columns= [
        {
            "label": frappe._("Batch No."),
            "fieldtype": "Link",
            "fieldname": "batch",
            "options": "Batch",
            "width": 200,
        },
		{
            "label": frappe._("Stock Entry"),
            "fieldtype": "Link",
            "fieldname": "stock_entry",
            "options": "Stock Entry",
            "width": 200,
        }

	]
	items=frappe.db.sql("""select distinct(si.item_name) as item_name from `tabStock Entry` se join `tabStock Entry Item` si ON  se.name=si.parent where se.stock_entry_type=='Manufacture' and docstaus=1 {0}""".format(condition),as_dict=1)
	for item in items:
		columns.append({
			{
            "label": frappe._(str(item.item_name)),
            "fieldtype": "Float",
            "fieldname": str(item.item_name),
            "width": 200,
        }
		})
	columns.extend([
		{
            "label": frappe._("Total Raw"),
            "fieldtype": "Float",
            "fieldname": "total",
            "width": 200,
        },
		{
            "label": frappe._("Finished Good"),
            "fieldtype": "Float",
            "fieldname": "finish_total",
            "width": 200,
        },
		{
            "label": frappe._("Rejected"),
            "fieldtype": "Float",
            "fieldname": "rejected",
            "width": 200,
        },
		{
            "label": frappe._("MA"),
            "fieldtype": "Float",
            "fieldname": "ma",
            "width": 200,
        },
		{
            "label": frappe._("ACT"),
            "fieldtype": "Float",
            "fieldname": "act",
            "width": 200,
        },
	])
       
	return columns





def get_data(filters):
	condition=""
	if filters.from_date and filters.to_date:
		condition+="and se.transaction_date>='{0}' and se.transaction_date<='{1}'".format(filters.from_date ,filters.to_date)
	items=frappe.db.sql("""select distinct(si.batch_no) as batch from `tabStock Entry` se join `tabStock Entry Item` si ON  se.name=si.parent where se.stock_entry_type=='Manufacture' and se.docstaus=1 {0}""".format(condition),as_dict=1)
	data=[]
	for item in items:
		
		parents=frappe.db.sql("""select distinct(si.parent) as parent from `tabStock Entry` se join `tabStock Entry Item` si ON  se.name=si.parent where se.stock_entry_type=='Manufacture' and se.docstaus=1 and si.batch_no='{0}' {1}""".format(item.get("batch"),condition),as_dict=1)
		for pa in parents:
			raw=0
			finish_good=0
			rejected=0
			act=0
			values={}
			doc=frappe.get_doc("Stock Entry",pa.get("parent"))
			
			values.update({"batch":item.get("batch"),"rate":0,"stock_entry":doc.name,"rate":0})
			for i in doc.items:
				qty=frappe.db.sql("""select sum(si.qty) as qty ,avg(si.rate) as rate,sum(se) from `tabStock Entry` se join `tabStock Entry Item` si ON  se.name=si.parent where se.stock_entry_type=='Manufacture' and se.docstaus=1 and si.batch_no='{0}' and si.item_code='{1}' '{2}'""".format(item.get("batch"),i.item_code,pa.get("parent"),condition),as_dict=1)
				for val in qty:
					values.update({
						str(i.item_name):flt(values.get(str(i.item_name)))+val.get("qty"),
						"rate":flt(values.get("rate"))+val.get("rate")
					})
					
					
			raw=raw+doc.total_input_qty
			finish_good=finish_good+doc.total_output_qty
			rejected=raw-doc.total_in_over_qty
			act=finish_good+rejected
			values.update({
				"total":raw,
				"finish_total":finish_good,
				"rejected":rejected,
				"ma":0,
				"act":act
				})
			data.append(values)
	return data
	
		


		


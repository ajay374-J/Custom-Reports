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
		condition+="and se.posting_date>='{0}' and se.posting_date<='{1}'".format(filters.from_date ,filters.to_date)
	
	columns= [
        {
            "label": frappe._("Batch No."),
            "fieldtype": "Link",
            "fieldname": "batch",
            "options": "Batch",
            "width": 200,
        }
	]
	items=frappe.db.sql("""select distinct(si.item_name) as item_name from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1  and t_warehouse is NULL {0}""".format(condition),as_dict=1)
	for item in items:
		columns.append(
			{
            "label": frappe._(str(item.item_name)),
            "fieldtype": "Float",
            "fieldname": str(item.item_name),
            "width": 200,
        }
		)
	columns.extend([
		{
            "label": frappe._("Total IN"),
            "fieldtype": "Float",
            "fieldname": "total",
            "width": 200,
        },
		{
            "label": frappe._("Alloy"),
            "fieldtype": "Link",
            "fieldname": "alloy",
            "options": "Item",
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
            "label": frappe._("ACT"),
            "fieldtype": "Float",
            "fieldname": "act",
            "width": 200,
        },
		{
            "label": frappe._("Exp"),
            "fieldtype": "Float",
            "fieldname": "exp",
            "width": 200,
        },
		{
            "label": frappe._("Diff"),
            "fieldtype": "Float",
            "fieldname": "diff",
            "width": 200,
        },
		{
            "label": frappe._("Supervisor"),
            "fieldtype": "Data",
            "fieldname": "supervisor",
            "width": 200,
        },
		{
            "label": frappe._("Price"),
            "fieldtype": "Currency",
            "fieldname": "rate",
            "width": 200,
        },
	])
       
	return columns





def get_data(filters):
	condition=""
	if filters.from_date and filters.to_date:
		condition+="and se.posting_date>='{0}' and se.posting_date<='{1}'".format(filters.from_date ,filters.to_date)
	items=frappe.db.sql("""select distinct(si.batch_no) as batch from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1 {0}""".format(condition),as_dict=1)
	data=[]
	for item in items:
		if item.get("batch"):
			raw=0
			finish_good=0
			recovery=0
			act=0
			reject=0
			diff=0
			exp=0
			values={}
			parents=frappe.db.sql("""select distinct(si.parent) as parent from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1 and si.batch_no='{0}' {1}""".format(item.get("batch"),condition),as_dict=1)
			supervisor=""
			for pa in parents:

				doc=frappe.get_doc("Stock Entry",pa.get("parent"))
				
				for su in doc.supervisor:
					supervisor+=str(su.name1)+","

				values.update({"batch":item.get("batch")})
				qty=0
				for i in doc.items:
					qty+=i.qty
					rate=frappe.db.sql("""select avg(si.basic_rate) as rate  from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1 and si.batch_no='{0}' and si.item_code='{1}' and se.name='{2}' {3}""".format(item.get("batch"),i.item_code,pa.get("parent"),condition),as_dict=1)
					
					
					values.update({
						str(i.item_name):flt(values.get(str(i.item_name)))+flt(i.get("qty")),
					})
					for ra in rate:
						values.update({
							"rate":flt(values.get("rate"))+flt(ra.get("rate"))
						})
					
				rejected=frappe.db.sql("""select sum(si.qty) as qty  from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1 and si.batch_no='{0}' and se.name='{1}' and si.item_in_overall=1 and si.is_scrap_item=1 {2}""".format(item.get("batch"),pa.get("parent"),condition),as_dict=1)
				# item=frappe.db.get_value("")
				if rejected:
					rejects=rejected[0].get("qty")
				raw=flt(raw)+flt(doc.total_input_qty)
				finish_good=flt(finish_good)+flt(doc.total_output_qty)
				reject+=flt(rejects)
				exp+=flt(doc.custom_total_expected_qty)
				act+=flt(doc.total_in_over_qty)
				diff=act-exp
			values.update({
				"total":raw,
				"finish_total":finish_good,
				"rejected":reject,
				"act":act,
				"exp":exp,
				"diff":diff,
				"supervisor":supervisor
				})
			data.append(values)	
	

	from collections import defaultdict

	result = defaultdict(float)

	for row in data:
		for k, v in row.items():
			if isinstance(v, float):
				result[str(k)] += v

	# Add your custom label after aggregation
	result["batch"] = "<b>Total</b>"

	# Convert to dict if needed
	result = dict(result)

	# If you want it as a regular dict:
	result = dict(result)
	data.append(result)
	data.append({})
	rate_dic={"batch":"<b>PRICE/MT</b>"}
	rates=frappe.db.sql("select item_name,avg(basic_rate) as rate from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1 group by item_name")
	for i in rates:
		rate_dic.update({str(i.item_name):i.get("rate")})

	return data
	
		


		


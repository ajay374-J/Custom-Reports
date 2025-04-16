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
            "fieldtype": "Data",
            "fieldname": "batch",
            "width": 200,
        }
	]
	items=frappe.db.sql("""select distinct(si.item_name) as item_name from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1  and t_warehouse is NULL and (si.item_in_overall=0 and si.is_scrap_item=0 or si.item_in_overall=1)  {0}""".format(condition),as_dict=1)
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
					employee_name=frappe.db.get_value("Employee",su.name1,"employee_name")
					supervisor+=str(employee_name)+","

				values.update({"batch":item.get("batch"),"rate":0})
				qty=0
				for i in doc.items:
					qty+=i.qty					
					
					values.update({
						str(i.item_name):flt(values.get(str(i.item_name)))+flt(i.get("qty")),
					})
					
					if i.batch_no  and i.t_warehouse:
						values.update({
							"alloy":i.item_code
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
			rate=frappe.db.sql("""select avg(si.basic_rate) as rate  from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1 and si.batch_no='{0}' {1}""".format(item.get("batch"),condition),as_dict=1)
			for ra in rate:
				values.update({
					"rate":flt(values.get("rate"))+flt(ra.get("rate"))
				})
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
	rate_dic={"batch":"<b>PRICE/MT</b>"}
	rates = frappe.db.sql("""
    SELECT item_name, AVG(basic_rate) AS rate
    FROM `tabStock Entry` se
    JOIN `tabStock Entry Detail` si ON se.name = si.parent
    WHERE se.stock_entry_type = 'Manufacture' AND se.docstatus = 1 {0}
    GROUP BY item_name
	""".format(condition), as_dict=1)

	print("############################# RATES RAW:", rates)

	for jk in rates:
		item = str(jk.get("item_name"))
		rate = jk.get("rate") or 0
		rate_dic[item] = rate

	print("########## FINAL RATE DIC:", rate_dic)

	data.append(rate_dic)

	return data
	
		


		


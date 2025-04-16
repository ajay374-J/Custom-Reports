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
            "label": frappe._("FG Item"),
            "fieldtype": "Data",
            "fieldname": "fg_item",
            "width": 200,
        }
	]
	items=frappe.db.sql("""select distinct(si.item_name) as item_name from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1  and t_warehouse is NULL and si.item_in_overall=0 and si.is_scrap_item=0 and si.is_finished_item=0 {0}""".format(condition),as_dict=1)
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
		# {
        #     "label": frappe._("Alloy"),
        #     "fieldtype": "Link",
        #     "fieldname": "alloy",
        #     "options": "Item",
        #     "width": 200,
        # },
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
		{
            "label": frappe._("FG Total Value"),
            "fieldtype": "Currency",
            "fieldname": "tot_value",
            "width": 200,
        },
	])
       
	return columns





def get_data(filters):
	condition=""
	if filters.from_date and filters.to_date:
		condition+="and se.posting_date>='{0}' and se.posting_date<='{1}'".format(filters.from_date ,filters.to_date)
	items=frappe.db.sql("""select distinct(si.item_code) as item from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1 and si.is_finished_item =1""".format(condition),as_dict=1)
	data=[]
	for item in items:
		if item.get("item"):
			raw=0
			finish_good=0
			act=0
			reject=0
			diff=0
			exp=0
			values={}
			parents=frappe.db.sql("""select distinct(si.parent) as parent from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1 and si.item_code='{0}' {1}""".format(item.get("item"),condition),as_dict=1)
			supervisor=""
			values.update({"fg_item":item.get("item"),"rate":0})
			for pa in parents:
				
				doc=frappe.get_doc("Stock Entry",pa.get("parent"))
				
				for su in doc.supervisor:
					employee_name=frappe.db.get_value("Employee",su.name1,"employee_name")
					supervisor+=str(employee_name)+","

				qty=0

				for i in doc.items:
					if i.item_in_overall==0 and i.is_scrap_item==0 and i.is_finished_item==0:
						qty+=i.qty
						
						
						values.update({
							str(i.item_name):flt(values.get(str(i.item_name)))+flt(i.get("qty")),
						})

					# if i.batch_no  and i.target_warehouse:
					# 	values.update({
					# 		"alloy":i.item_code
					# 	})
					
				rejected=frappe.db.sql("""select sum(si.qty) as qty  from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1  and se.name='{0}' and si.item_in_overall=1 and si.is_scrap_item=1 {1}""".format(pa.get("parent"),condition),as_dict=1)
				# item=frappe.db.get_value("")
				if rejected:
					rejects=rejected[0].get("qty")
				raw=flt(raw)+flt(doc.total_input_qty)
				finish_good=flt(finish_good)+flt(doc.total_output_qty)
				reject+=flt(rejects)
				exp+=flt(doc.custom_total_expected_qty)
				act+=flt(doc.total_in_over_qty)
				diff=act-exp
			frate=0
			rate=frappe.db.sql("""select avg(si.basic_rate) as rate  from `tabStock Entry` se join `tabStock Entry Detail` si ON  se.name=si.parent where se.stock_entry_type='Manufacture' and se.docstatus=1 and si.item_code='{0}' {1}""".format(item.get("item"),condition),as_dict=1)
			for ra in rate:
				frate=flt(values.get("rate"))+flt(ra.get("rate"))
				values.update({
					"rate":frate
				})
			values.update({
				"total":raw,
				"finish_total":finish_good,
				"rejected":reject,
				"act":act,
				"exp":exp,
				"diff":diff,
				"supervisor":supervisor,
				"tot_value":flt(finish_good)*flt(frate)

				})
			data.append(values)	
	

	from collections import defaultdict

	result = defaultdict(float)

	for row in data:
		for k, v in row.items():
			if isinstance(v, float):
				result[str(k)] += v

	# Add your custom label after aggregation
	result["fg_item"] = "<b>Total</b>"

	# Convert to dict if needed
	result = dict(result)

	# If you want it as a regular dict:
	result = dict(result)
	data.append(result)
	rate_dic={"fg_item":"<b>PRICE/MT</b>"}
	rates = frappe.db.sql("""
    SELECT item_name, AVG(basic_rate) AS rate
    FROM `tabStock Entry` se
    JOIN `tabStock Entry Detail` si ON se.name = si.parent
    WHERE se.stock_entry_type = 'Manufacture' AND se.docstatus = 1 {0}
    GROUP BY item_name
	""".format(condition), as_dict=1)


	for jk in rates:
		item = str(jk.get("item_name"))
		rate = jk.get("rate") or 0
		rate_dic[item] = rate


	data.append(rate_dic)
	total_value={"fg_item":"<b>Total Value</b>","rate":0}


	dict1=data[-2]
	dict2=data[-1]
	

	for key in dict1:
		if key == 'z':
			total_value[key] = str(dict1[key]) + str(dict2[key])
		else:
			total_value[key] = dict1[key] * dict2[key]
	data.append(total_value)

	return data
	
		


		


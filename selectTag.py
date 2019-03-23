# ******************************************************************************
# * Copyright of this product 2013-2023,
# * MACHBASE Corporation(or Inc.) or its subsidiaries.
# * All Rights reserved.
# ******************************************************************************

 import re
import json
from machbaseAPI import machbase

def get_eq_by_tag(db, tag_name):
    db = machbase()
    if db.open('127.0.0.1','SYS','MANAGER',5656) is 0 :
        return db.result()
    query = "select equipment_id from tag_equipment where tag_name = '"+tag_name + "'"
    if db.execute(query) is 0:
       return db.result()
    result = db.result()
    for item in re.findall('{[^}]+}',result):
        res = json.loads(item)
        result = res.get("equipment_id")
    return result

def get_time_lotno_by_eq_lotid(db, eq_id, lot_id):
    query = "select enter_time, out_time, lot_no from process_data where lot_id = '" + lot_id + "' AND equipment_id = '"+eq_id +"'"
    print query
    if db.execute(query) is 0:
       return db.result()
    result = db.result()
    for item in re.findall('{[^}]+}',result):
        res = json.loads(item)
        t1 = res.get("enter_time")
        t2 = res.get("out_time")
        n3 = res.get("lot_no")
    return [t1, t2, n3]

def get_tagdata_by_tagtime_lotno(db, tag, tfrom, to, lot_no):
    query = "select * from tag where name = '" + tag + "' and time between to_date('" + tfrom + "') and to_date('" + to+"')" + "and lot_no = " + lot_no;
    if db.execute(query) is 0:
       return db.result()
    result = db.result()
    for item in re.findall('{[^}]+}',result):
         print item
    return result

def query_eq_lot(db, eq, lot_id):
    querytagname = "select tag_name from tag_equipment where equipment_id = '"+eq+"'"
    tfrom, to, lot_no = get_time_lotno_by_eq_lotid(db, eq, lot_id)
    if db.execute(querytagname) is 0:
       return db.result()
    resloop = db.result()
    for item in re.findall('{[^}]+}',resloop):
        res = json.loads(item)
        tagname = res.get("tag_name")
        result = get_tagdata_by_tagtime_lotno(db, tagname, tfrom, to, lot_no)
    return result


if __name__=="__main__":
    db = machbase()
    if db.open('127.0.0.1','SYS','MANAGER',5656) is 0 :
        print("eror connection")
    print get_eq_by_tag(db, "EQ0^TAG1")
    print get_time_lotno_by_eq_lotid(db, get_eq_by_tag(db, "EQ0^TAG1"), "LOT101")
    tfrom, to, lot_no =  get_time_lotno_by_eq_lotid(db, get_eq_by_tag(db, "EQ0^TAG1"), "LOT101")
    print get_tagdata_by_tagtime_lotno(db, "EQ0^TAG1", tfrom, to, lot_no)
    print(query_eq_lot(db, "EQ0", "LOT202"))
    if db.close() is 0 :
       print("disconnect error")

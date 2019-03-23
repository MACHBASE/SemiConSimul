create table process_data (lot_id varchar(40), equipment_id varchar(40),
                           enter_time datetime, out_time datetime, lot_no long);
create index processdata_lot on process_data(lot_id);

create lookup table tag_equipment (
    tag_name varchar(20),
    equipment_id varchar(40)
    );
create index tag_eq_tag on tag_equipment(tag_name);
create index tag_eq_eq  on tag_equipment(equipment_id);

create tagdata table tag (name varchar(20) primary key, time datetime basetime, value double summarized, lot_no long);

source ./env.sh
machadmin -k; echo 'y'|machadmin -d; machadmin -c; machadmin -u
machsql -s localhost -u sys -p manager -f create_table.sql
python ./gen_tag.py > create_tag_equip_list.sql
machsql -s localhost -u sys -p manager -f create_tag_equip_list.sql
export NO_LOTNO=0
make
./append

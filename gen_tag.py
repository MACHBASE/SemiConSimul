# ******************************************************************************
# * Copyright of this product 2013-2023,
# * MACHBASE Corporation(or Inc.) or its subsidiaries.
# * All Rights reserved.
# ******************************************************************************

import os
eqc = int(os.getenv('TEST_EQUIP_CNT', 10))
tgc = int(os.getenv('TEST_TAG_PER_EQ', 1000))
loopc = eqc * tgc
eq = 0
for x in range(0, loopc):
    print "insert into tag metadata values(\'EQ%d^TAG%d\');" % (eq, x)
    print "insert into tag_equipment values(\'EQ%d^TAG%d\', \'EQ%d\');" % (eq, x, eq)
    if (int(x) % int(tgc) == 0 and x != 0 ):
        eq = eq + 1

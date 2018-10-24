
import pyspark
from pyspark import SparkContext
import os
import sys
from collections import defaultdict

sc = SparkContext('local','example')

support = int(sys.argv[-1])
fn2 = sys.argv[-2]
fn1 = sys.argv[-3]
case = int(sys.argv[-4])
if os.path.exists(fn1):
    d1 = sc.textFile(fn1)
if os.path.exists(fn2):
    d0 = sc.textFile(fn2)

def get_ratings_tuple(entry):
   
    items = entry.split('::')
    return int(items[0]), int(items[1])

def get_users_tuple(entry):
    items = entry.split('::')
    return int(items[0]) , items[1]

def del_m(items):
    return items[0],items[1][1]

def del_u(items):
    return items[1]

def count_in_a_partition(idx, iterator):
    count = 0
    for _ in iterator:
        count += 1
    return idx, count

def ff_del_m(items):
    return items[1][0],items[0]

def ff_del_u(items):
    return items[1]

ratingsRDD = d0.map(get_ratings_tuple)
usersRDD = d1.map(get_users_tuple)

if case == 1:
    ans = usersRDD.join(ratingsRDD)
    ans_f = ans.filter(lambda x: 'M' in x[1])

    ans_g = ans_f.map(del_m).distinct().groupByKey().map(del_u)
    num = len(ans_g.mapPartitionsWithIndex(count_in_a_partition).collect())/2

elif case == 2:
    ff_ans = ratingsRDD.join(usersRDD)
    ff_ans_f = ff_ans.filter(lambda x: 'F' in x[1][1])

    ff_ans_g = ff_ans_f.map(ff_del_m).distinct().groupByKey().map(ff_del_u)
    num = len(ff_ans_g.mapPartitionsWithIndex(count_in_a_partition).collect())/2


def apriori_n(iterator):
    count = 0
    order = 2
    l = set()
    tmp_lst = list()
    freq = dict()
    freq_l = []
    item_count = defaultdict(lambda: 0)
    chunk_s = support/num
    for x in iterator:
        tmp = set(x)
        tmp_lst.append(tmp)
        for y in x:
            item_count[y]+=1
    for item, count in item_count.iteritems():
        if count >= chunk_s:
            l.add(frozenset([item]))
    
    freq[order-1] = l
    while l!= set([]):
        l = set([a.union(b) for a in l for b in l if len(a.union(b)) == order])
        c = freq_search(tmp_lst, chunk_s, l)
        if c != set([]): 
            freq[order] = c
        order+=1
        l = c
    for zz in freq.itervalues():
        for qq in zz:
            freq_l.append(qq)
    return freq_l

def freq_search(tmp_lst, chunk_s, l):
    item_dict = defaultdict(int)
    frq_lst = set()
    for ll in l:
        for tmp in tmp_lst:
            if ll.issubset(tmp):
                item_dict[ll] += 1

    for i, count in item_dict.items():
        if count >= chunk_s:
            frq_lst.add(i)
    return frq_lst

if case ==1:
 
    ans_g_up = ans_g.mapPartitions(apriori_n).collect()
    rdd_g_up = sc.parallelize(ans_g_up)
    rdd_add = rdd_g_up.map(lambda x: (x,1))
    rdd_comb = rdd_add.reduceByKey(lambda x,y: x).map(lambda (x,y): x)
    rdd_comb = rdd_comb.collect()
    check_all = sc.broadcast(rdd_comb)
    aans = ans_g.map(set).persist()
    aans_add = aans.flatMap(lambda line: [(x,1) for x in check_all.value if line.issuperset(x)])

    aans_red = aans_add.reduceByKey(lambda x,y: x+y).filter(lambda (i,x): x >= support).map(lambda x: x[0])
    finale = aans_red.collect()

    fin_list = ([list(x) for x in finale])

    new_fin = sorted(fin_list, key = len)
    
    lst = []
    big_lst = []
    
    for i in range(1, 1+len(max(fin_list, key = len))):
        for item in fin_list:
            item = sorted(item)
            if len(item) == i:
                lst.append(item)
    
        big_lst.append(sorted(lst))
        lst = []
    my_lst = [] 
    for z in big_lst:
        my_lst.append(([tuple(rr) for rr in z]))
    strr = ""
    with open('SON.case1.txt', 'w') as f:
        for i in range(len(big_lst)):
            if i == 0:
                for x in range(len(big_lst[i])):
                    if x < (len(big_lst[i]) -1):
                        f.writelines("(%s), " %big_lst[i][x][0])
                    else:
                        f.writelines("(%s)\n" %big_lst[i][x][0])
                
            else:
                for x in range(len(my_lst[i])):
                    if x < (len(my_lst[i]) -1):
                        f.writelines("%s, " %str(my_lst[i][x]))
                    else:
                        f.writelines("%s\n" %str(my_lst[i][x]))
    
elif case == 2:
    
    ff_ans_g_up = ff_ans_g.mapPartitions(apriori_n).collect()
    ff_rdd_g_up = sc.parallelize(ff_ans_g_up)
    ff_rdd_add = ff_rdd_g_up.map(lambda x: (x,1))
    ff_rdd_comb = ff_rdd_add.reduceByKey(lambda x,y: x).map(lambda (x,y): x)
    ff_rdd_comb = ff_rdd_comb.collect()
    ff_check_all = sc.broadcast(ff_rdd_comb)
    ff_aans = ff_ans_g.map(set).persist()
    ff_aans_add = ff_aans.flatMap(lambda line: [(ff_x,1) for ff_x in ff_check_all.value if line.issuperset(ff_x)])

    ff_aans_red = ff_aans_add.reduceByKey(lambda x,y: x+y).filter(lambda (i,x): x >= support).map(lambda x: x[0])
    ff_finale = ff_aans_red.collect()

    ff_fin_list = ([list(x) for x in ff_finale])

    ff_new_fin = sorted(ff_fin_list, key = len)
    
    lst = []
    big_lst = []
    
    for i in range(1, 1+len(max(ff_fin_list, key = len))):
        for item in ff_fin_list:
            item = sorted(item)
            if len(item) == i:
                lst.append(item)
    
        big_lst.append(sorted(lst))
        lst = []
    my_lst = [] 
    for z in big_lst:
        my_lst.append(([tuple(rr) for rr in z]))
    with open('SON.case2.txt', 'w') as f:
        for i in range(len(big_lst)):
            if i == 0:
                for x in range(len(big_lst[i])):
                    if x < (len(big_lst[i]) -1):
                        f.writelines("(%s), " %big_lst[i][x][0])
                    else:
                        f.writelines("(%s)\n" %big_lst[i][x][0])
                
            else:
                for x in range(len(my_lst[i])):
                    if x < (len(my_lst[i]) -1):
                        f.writelines("%s, " %str(my_lst[i][x]))
                    else:
                        f.writelines("%s\n" %str(my_lst[i][x]))
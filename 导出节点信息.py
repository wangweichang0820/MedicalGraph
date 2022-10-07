# coding: utf-8

from py2neo import *


# 负责从Neo4j中查询出建立树状图所需要的数据
class treeSql():

    def __init__(self):
        self.graph = Graph("http://localhost:7474", auth=("neo4j", "neo4j"))

    '''
    zq数据库导出
    '''

    def zq(self):
        sql = 'match (n:`药物`) return n'
        # sql = 'match (m)-[r]-(n) return m,r,n'
        res = self.graph.run(sql).data()
        return res


# 负责从Neo4j中查询出建立图谱所需要的数据
if __name__ == "__main__":
    pass
    t = treeSql()
    s = t.zq()
    print(s)
    with open('药物.txt', 'w', encoding='utf8') as w:
        for i in range(len(s)):
            w.write(str(s[i]))
            w.write('\n')

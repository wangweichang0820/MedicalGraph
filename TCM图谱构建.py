# coding: utf-8
from py2neo import Graph, Node, Relationship
import pandas as pd
import re
import os


class MedicalGraph:
    def __init__(self):

        self.graph = Graph("http://localhost:7474", auth=("neo4j", "123456"))

    @property
    def read_file(self):
        """
        读取文件，获得实体，实体关系
        :return:
        """
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        # TCM数据文件路径
        zf_zz = os.path.join(cur_dir, 'data/tcm/治法-证候.txt')
        zf_zy = os.path.join(cur_dir, 'data/tcm/治法-作用.txt')
        # zf_zfl = os.path.join(cur_dir, 'data/tcm/治法-治法类.txt')
        jb_by = os.path.join(cur_dir, 'data/tcm/疾病-病因.txt')
        jb_bl = os.path.join(cur_dir, 'data/tcm/疾病-病类.txt')
        jb_zz = os.path.join(cur_dir, 'data/tcm/疾病-症状.txt')
        zh_zz = os.path.join(cur_dir, 'data/tcm/证候-症状.txt')
        # zh_zl = os.path.join(cur_dir, 'data/tcm/证候-证类.txt')

        # 方剂数据文件路径
        pre_dir = os.path.join(cur_dir, 'data/prescription1.csv')
        # 药物数据文件路径
        drug_dir = os.path.join(cur_dir, 'data/drug.csv')

        zf_zh_data = pd.read_csv(zf_zz, encoding='utf8', sep=" ").loc[:, :].values
        zf_zy_data = pd.read_csv(zf_zy, encoding='utf8', sep=" ").loc[:, :].values
        # zf_zfl_data = pd.read_csv(zf_zfl, encoding='utf8', sep=" ").loc[:, :].values
        jb_by_data = pd.read_csv(jb_by, encoding='utf8', sep=" ").loc[:, :].values
        jb_bl_data = pd.read_csv(jb_bl, encoding='utf8', sep=" ").loc[:, :].values
        jb_zz_data = pd.read_csv(jb_zz, encoding='utf8', sep=" ").loc[:, :].values
        zh_zz_data = pd.read_csv(zh_zz, encoding='utf8', sep=" ").loc[:, :].values
        # zh_zl_data = pd.read_csv(zh_zl, encoding='utf8', sep=" ").loc[:, :].values
        pre_data = pd.read_csv(pre_dir, encoding='gb18030').loc[:, :].values
        drug_data = pd.read_csv(drug_dir, encoding='utf8').loc[:, :].values

        # TCM变量集合
        treatment_set = []  # 治法名称
        syndromes_set = []  # 证候
        disease_set = []  # 疾病
        effect_set = []  # 作用
        # type_of_treatment_set = []  # 治法类
        cause_set = []  # 病因
        disease_category_set = []  # 病类
        symptom_set = []  # 症状
        # syn_category_set = []  # 证类

        # cols = ["方剂名称", "典籍", "别名", "配方", "用法用量", "组成药品", "针对症状"]
        # 方剂的属性：别名，配方，用法用量
        prescriptions_infos = []
        # 药物的属性：药物名，别名
        # drug_infos = []

        prescriptions = []  # 方剂名称
        book_set = []  # 典籍
        drug_set = []  # 组成药物

        taste_set = []  # 药物性味
        meridians_set = []  # 经络

        # 关系
        prescription_book = []  # 方剂-典籍 来源
        prescription_drug = []  # 方剂-组成药品
        prescription_effect = []  # 方剂-作用
        prescription_symptom = []  # 方剂-针对症状

        drug_taste = []  # 药物性味
        drug_meridians = []  # 药物经络
        drug_effect = []  # 药物作用
        drug_symptom = []  # 药物主治症状
        drug_book = []  # 药物出自典籍

        tre_eff_rel = []  # 治法作用关系
        tre_syn_rel = []  # 治法证候关系
        # type_of_tre_rel = []  # 治法治法类关系
        cause_of_dis_rel = []  # 疾病病因关系
        type_of_dis_rel = []  # 疾病病类关系
        dis_sym_rel = []  # 疾病症状关系
        syn_sym_rel = []  # 证候症状关系
        type_of_syn_rel = []  # 证候证类关系

        for data in zf_zh_data:
            zf = str(data[0]).strip().split()
            zh = str(data[2]).strip().split()
            for i in zf:
                treatment_set.append(i)
                for j in zh:
                    syndromes_set.append(j)
                    tre_syn_rel.append([i, j])
        # print(tre_syn_rel, len(tre_syn_rel))

        for data in zf_zy_data:
            zf = str(data[0]).strip().split()
            zy = str(data[2]).strip().split()
            for i in zf:
                treatment_set.append(i)
            for j in zy:
                effect_set.append(j)
                tre_eff_rel.append([i, j])
        # print(tre_eff_rel, len(tre_eff_rel))

        # for data in zf_zfl_data:
        #     zf = str(data[0]).strip().split()
        #     zfl = str(data[2]).strip().split()
        #     for i in zf:
        #         treatment_set.append(i)
        #     for j in zfl:
        #         effect_set.append(j)
        #         type_of_tre_rel.append([i, j])
        # print(type_of_tre_rel, len(type_of_tre_rel))

        for data in jb_by_data:
            jb = str(data[0]).strip().split()
            by = str(data[2]).strip().split()
            for i in jb:
                disease_set.append(i)
            for j in by:
                cause_set.append(j)
                cause_of_dis_rel.append([i, j])
        # print(cause_of_dis_rel, len(cause_of_dis_rel))

        for data in jb_bl_data:
            jb = str(data[0]).strip().split()
            bl = str(data[2]).strip().split()
            for i in jb:
                disease_set.append(i)
            for j in bl:
                disease_category_set.append(j)
                type_of_dis_rel.append([i, j])
        # print(type_of_dis_rel, len(type_of_dis_rel))

        for data in jb_zz_data:
            jb = str(data[0]).strip().split()
            zz = str(data[2]).strip().split()
            for i in jb:
                disease_set.append(i)
            for j in zz:
                symptom_set.append(j)
                dis_sym_rel.append([i, j])
        # print(dis_sym_rel, len(dis_sym_rel))

        for data in zh_zz_data:
            zh = str(data[0]).strip().split()
            zz = str(data[2]).strip().split()
            for i in zh:
                syndromes_set.append(i)
            for j in zz:
                symptom_set.append(j)
                syn_sym_rel.append([i, j])
        # print(syn_sym_rel, len(syn_sym_rel))

        # for data in zh_zl_data:
        #     zh = str(data[0]).strip().split()
        #     zl = str(data[2]).strip().split()
        #
        #     for i in zh:
        #         syndromes_set.append(i)
        #     for j in zl:
        #         syn_category_set.append(j)
        #         type_of_syn_rel.append([i, j])

        # print(type_of_syn_rel, len(type_of_syn_rel))

        for data in pre_data:
            prescription_dict = {}  # 方剂节点信息
            # 方剂
            # prescription = str(data[0]).strip().split()
            prescription = str(data[0]).strip()
            # print(prescription)
            prescription_dict["节点名称"] = prescription
            prescription_dict["方剂名称"] = str(data[1]).strip()

            # 出处
            prescription_dict["出自典籍"] = str(data[2]).strip() if str(data[2]) else ""

            # 别名
            prescription_dict["别名"] = str(data[3]).strip() if str(data[3]) else ""

            # 配方
            prescription_dict["配方"] = str(data[4]).strip() if str(data[4]) else ""

            # 功效
            effect_list = str(data[5]).strip().split() if str(data[5]) else ""
            for effect in effect_list:
                effect_set.append(effect)
                prescription_effect.append([prescription, effect])

            # 用法用量
            prescription_dict["用法用量"] = str(data[6]).strip() if str(data[6]) else ""

            # 典籍
            book_list = str(data[7]).strip().split() if str(data[7]) else ""
            for book in book_list:
                book_set.append(book)
                prescription_book.append([prescription, book])

            # 组成药品
            drug_list = str(data[8]).strip().split() if str(data[8]) else ""
            for drug in drug_list:
                drug_set.append(drug)
                prescription_drug.append([prescription, drug])

            # 主治症状
            symptom_list = str(data[9]).strip().split() if str(data[9]) else ""
            for symptom in symptom_list:
                symptom_set.append(symptom)
                prescription_symptom.append([prescription, symptom])

            prescriptions_infos.append(prescription_dict)

        for data in drug_data:
            # drug_dict = {}  # 药物节点信息

            drug = str(data[0]).strip()
            drug_set.append(drug)
            # drug_dict["药物名称"] = drug

            # 别名
            # drug_dict["别名"] = str(data[1]).strip() if str(data[1]) else ""
            # drug_infos.append(drug_dict)

            # 性味
            for taste in str(data[2]).strip().split():
                taste_set.append(taste)
                drug_taste.append([drug, taste])

            # 经络
            for meridians in str(data[3]).strip().split():
                meridians_set.append(meridians)
                drug_meridians.append([drug, meridians])

            # 功效
            for effect in str(data[4]).strip().split():
                effect_set.append(effect)
                drug_effect.append([drug, effect])

            # 主治病症
            for symptom in str(data[5]).strip().split():
                symptom_set.append(symptom)
                drug_symptom.append([drug, symptom])

            # 出自典籍
            for book in str(data[6]).strip().split():
                book_set.append(book)
                drug_book.append([drug, book])

        return set(treatment_set), set(syndromes_set), set(disease_set), set(effect_set), set(cause_set), set(
            disease_category_set), set(symptom_set), set(book_set), set(drug_set), set(taste_set), set(meridians_set), \
               tre_eff_rel, tre_syn_rel, cause_of_dis_rel, type_of_dis_rel, \
               dis_sym_rel, syn_sym_rel, type_of_syn_rel, prescription_effect, prescription_book, prescription_drug, \
               prescription_symptom, drug_taste, drug_meridians, drug_effect, drug_symptom, drug_book, \
               prescriptions_infos

    def create_node(self, label, nodes):
        """
        创建节点
        :param label: 标签
        :param nodes: 节点
        :return:
        """
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.graph.create(node)
            count += 1
            print(count, len(nodes))
        return

    def create_prescription_nodes(self, prescriptions_infos):
        """
        创建方剂节点的属性
        :param drug_info: list(Dict)
        :return:
        """
        count = 0
        for prescription_dict in prescriptions_infos:
            node = Node("方剂", name=prescription_dict['节点名称'], prename=prescription_dict['方剂名称'],
                        classics=prescription_dict['出自典籍'], alias=prescription_dict['别名'],
                        formula=prescription_dict['配方'], dosage=prescription_dict['用法用量'])
            self.graph.create(node)
            count += 1
            # print(count)
        return

    def create_graphNodes(self):
        """
        创建知识图谱实体
        :return:
        """
        treatment_set, syndromes_set, disease_set, effect_set, cause_set, disease_category_set, symptom_set, book_set, drug_set, taste_set, meridians_set, tre_eff_rel, tre_syn_rel, cause_of_dis_rel, type_of_dis_rel, dis_sym_rel, syn_sym_rel, type_of_syn_rel, prescription_effect, prescription_book, prescription_drug, prescription_symptom, drug_taste, drug_meridians, drug_effect, drug_symptom, drug_book, prescriptions_infos = self.read_file
        self.create_prescription_nodes(prescriptions_infos)
        self.create_node("治法", treatment_set)
        self.create_node("证候", syndromes_set)
        self.create_node("疾病", disease_set)
        self.create_node("作用", effect_set)
        # self.create_node("治法类", type_of_treatment_set)
        self.create_node("病因", cause_set)
        self.create_node("病类", disease_category_set)
        self.create_node("症状", symptom_set)
        # self.create_node("证类", syn_category_set)
        self.create_node("典籍", book_set)
        self.create_node("药物", drug_set)
        self.create_node("性味", taste_set)
        self.create_node("经络", meridians_set)

        return

    def create_graphRels(self):
        treatment_set, syndromes_set, disease_set, effect_set, cause_set, disease_category_set, symptom_set, book_set, drug_set, taste_set, meridians_set, tre_eff_rel, tre_syn_rel, cause_of_dis_rel, type_of_dis_rel, dis_sym_rel, syn_sym_rel, type_of_syn_rel, prescription_effect, prescription_book, prescription_drug, prescription_symptom, drug_taste, drug_meridians, drug_effect, drug_symptom, drug_book, prescriptions_infos = self.read_file
        self.create_relationship("治法", "作用", tre_eff_rel, "功效", "治法作用")
        self.create_relationship("治法", "证候", tre_syn_rel, "治法证候", "治法证候")
        # self.create_relationship("治法", "治法类", type_of_tre_rel, "治法类别", "治法类别")
        self.create_relationship("疾病", "病因", cause_of_dis_rel, "病因", "病因")
        self.create_relationship("疾病", "病类", type_of_dis_rel, "病类", "病类")
        self.create_relationship("疾病", "症状", dis_sym_rel, "症状", "疾病症状")
        self.create_relationship("证候", "症状", syn_sym_rel, "症状", "证候症状")
        # self.create_relationship("证候", "证类", type_of_syn_rel, "证候类别", "证候类别")
        self.create_relationship("方剂", "作用", prescription_effect, "功效", "方剂作用")
        self.create_relationship("方剂", "典籍", prescription_book, "出自", "方剂典籍")
        self.create_relationship("方剂", "药物", prescription_drug, "组成", "组成药物")
        self.create_relationship("方剂", "症状", prescription_symptom, "主治", "方剂症状")
        self.create_relationship("药物", "性味", drug_taste, "性味", "药物性味")
        self.create_relationship("药物", "经络", drug_meridians, "归经", "药物归经")
        self.create_relationship("药物", "作用", drug_effect, "功效", "药物作用")
        self.create_relationship("药物", "症状", drug_symptom, "主治", "药物症状")
        self.create_relationship("药物", "典籍", drug_book, "出自", "药物典籍")

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        """
        创建实体关系边
        :param start_node: 头节点
        :param end_node: 尾节点
        :param edges: 边
        :param rel_type: 边类型
        :param rel_name: 边名称
        :return:
        """
        count = 0
        # 去重处理
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                self.graph.run(query)
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
        return


if __name__ == "__main__":
    handler = MedicalGraph()
    handler.create_graphNodes()
    handler.create_graphRels()

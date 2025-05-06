import os
import json

"""
计划每个月更新一次
上次更新时间：20250428
"""
def classify_stocks_by_industry(codeA_file, codeB_file):
    """
    根据行业对股票代码进行分类，当行业下股票数超过50时使用子行业分类
    
    参数:
    codeA_file: A股代码与行业对应关系文件路径
    codeB_file: 北交所代码与行业对应关系文件路径
    
    返回:
    无，直接输出结果到控制台和文件
    """
    # 存储行业分类结果的字典
    industry_dict = {}
    # 存储子行业分类结果的字典
    sub_industry_dict = {}
    # 存储行业与子行业的映射关系
    industry_to_sub = {}
    
    # 检查文件是否存在
    print(f"A股文件是否存在: {os.path.exists(codeA_file)}")
    print(f"北交所文件是否存在: {os.path.exists(codeB_file)}")
    
    # 处理A股数据
    a_count = 0
    if os.path.exists(codeA_file):
        with open(codeA_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split('\t')
                if len(parts) >= 4:  # 现在有4列：代码、名称、行业、子行业
                    code = parts[0]
                    name = parts[1]
                    industry = parts[2]
                    sub_industry = parts[3]
                    
                    # 记录行业分类
                    if industry not in industry_dict:
                        industry_dict[industry] = []
                    industry_dict[industry].append(code)
                    
                    # 记录子行业分类
                    if sub_industry not in sub_industry_dict:
                        sub_industry_dict[sub_industry] = []
                    sub_industry_dict[sub_industry].append(code)
                    
                    # 记录行业与子行业的映射关系
                    if industry not in industry_to_sub:
                        industry_to_sub[industry] = set()
                    industry_to_sub[industry].add(sub_industry)
                    
                    a_count += 1
                else:
                    print(f"A股文件格式不正确的行: {line}")
    
    print(f"成功处理A股数据: {a_count}条")
    
    # 处理北交所数据
    b_count = 0
    if os.path.exists(codeB_file):
        with open(codeB_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split('\t')
                if len(parts) >= 4:  # 现在有4列：代码、名称、行业、子行业
                    code = parts[0]
                    name = parts[1]
                    industry = parts[2]
                    sub_industry = parts[3]
                    
                    # 记录行业分类
                    if industry not in industry_dict:
                        industry_dict[industry] = []
                    industry_dict[industry].append(code)
                    
                    # 记录子行业分类
                    if sub_industry not in sub_industry_dict:
                        sub_industry_dict[sub_industry] = []
                    sub_industry_dict[sub_industry].append(code)
                    
                    # 记录行业与子行业的映射关系
                    if industry not in industry_to_sub:
                        industry_to_sub[industry] = set()
                    industry_to_sub[industry].add(sub_industry)
                    
                    b_count += 1
                else:
                    print(f"北交所文件格式不正确的行: {line}")
    
    print(f"成功处理北交所数据: {b_count}条")
    
    # 合并行业和子行业的结果
    final_dict = {}
    
    # 处理行业分类结果
    for industry, codes in industry_dict.items():
        # 如果行业下的股票数超过50，则使用子行业分类
        if len(codes) > 50 and industry in industry_to_sub:
            for sub_industry in industry_to_sub[industry]:
                if sub_industry in sub_industry_dict:
                    final_dict[sub_industry] = sub_industry_dict[sub_industry]
        else:
            final_dict[industry] = codes
    
    print(f"最终分类中的类别数量: {len(final_dict)}")
    
    # 如果字典为空，直接返回
    if not final_dict:
        print("没有找到任何行业分类数据，请检查输入文件")
        return
    
    # 输出结果
    output_file = os.path.join(os.path.dirname(codeA_file), "industry_classification.txt")
    with open(output_file, 'w', encoding='utf-8') as f:
        for category, codes in sorted(final_dict.items(), key=lambda x: len(x[1]), reverse=True):
            count = len(codes)
            codes_str = json.dumps(codes, ensure_ascii=False)
            output_line = f"{category}\t{count}\t{codes_str}"
            print(output_line)
            f.write(output_line + '\n')
    
    print(f"分类结果已保存到: {output_file}")
    print(f"共有 {len(final_dict)} 个分类类别")

if __name__ == "__main__":
    # 文件路径
    codeA_file = r"codeA2industry"
    codeB_file = r"codeB2industry"
    
    # 执行分类
    classify_stocks_by_industry(codeA_file, codeB_file)
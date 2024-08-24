# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 09:17:50 2024

@author: Yiming.Zhou
"""
import tkinter as tk
from polars import read_csv,concat,read_excel
import polars as pl
import os
import time
from concurrent.futures import ThreadPoolExecutor
# 记录程序开始时间
start_time = time.time()
# 程序执行代码
# ...

# 其他代码和GUI部分不变
root = tk.Tk()
root.geometry("750x300")
tk.Label(root,text= "固定列名:").grid(row=0,column=0)
tk.Label(root,text= "添加列名:").grid(row=1,column=0)
tk.Label(root,text= "文件夹位置:").grid(row=2,column=0)
tk.Label(root,text="barcode:").grid(row=3,column=0)
tk.Label(root,text="barcode列名:").grid(row=4,column=0)
tk.Label(root,text="文件名筛选:").grid(row=5,column=0)
v1 = tk.StringVar()
v2 = tk.StringVar()
v3 = tk.StringVar()
v4 = tk.StringVar()
v5 = tk.StringVar()
v6=tk.StringVar()
e1 = tk.Entry(root, textvariable=v1)
e2 = tk.Entry(root, textvariable=v2)
e3 = tk.Entry(root, textvariable=v3)
e4 = tk.Entry(root, textvariable=v4)
e5 = tk.Entry(root, textvariable=v5)
e6=tk.Entry(root,textvariable=v6)
e1.grid(row=0, column=1, padx=10, pady=5,ipadx=100)
e2.grid(row=1, column=1, padx=10, pady=5,ipadx=100)
e3.grid(row=2, column=1, padx=10, pady=5,ipadx=100)
e4.grid(row=3, column=1, padx=10, pady=5,ipadx=100)
e5.grid(row=4, column=1, padx=10, pady=5,ipadx=100)
e6.grid(row=5, column=1, padx=10, pady=5,ipadx=100)

kernel_Num=0
# 创建滑轮
def update_quantity(value):
    global kernel_Num
    kernel_Num=quantity_scale.get()


quantity_scale = tk.Scale(root, from_=2, to=100, orient=tk.HORIZONTAL, command=update_quantity)
quantity_scale.grid(row=6, column=1, padx=10, pady=5,ipadx=100)

# 显示数量
quantity_label = tk.Label(root, text="线程数量")
quantity_label.grid(row=7, column=1, padx=10, pady=5,ipadx=100)


# 其他代码不变...
def read_csv_with_encoding(file_path):  
    df=None
    try:
        df = read_csv(file_path)  
    except Exception:
        if 'csv' in file_path:
            try:
                df=pl.read_csv(file_path,separator='\t')
                summ=[]
                lengt=0
                summ.append(df.columns[0].split(","))
                for i in range(len(df)):
                    t=df[i,0].split(',')
                    summ.append(t)
                    if lengt<len(t):
                        lengt=len(t)
                for i in summ:
                    i.extend([""]*(lengt-len(i)))
                data=list(zip(*summ[1:]))
                df=pl.DataFrame(data)
                n=0
                for i in summ[0]:
                    if i!='':
                        df=df.rename({df.columns[n]:i})
                        n+=1
            except Exception:
                pass
        else:
            try:  
                df = read_excel(file_path)  # 使用 polars 的 read_excel  
            except Exception as r:  
                print(r)  
    return df  
 
def get_desktop_path():
    if os.name == 'nt':  # Windows
        import winreg
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r'Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders')
        return winreg.QueryValueEx(key, "Desktop")[0]
    else:  # macOS, Linux, etc.
        return os.path.join(os.path.expanduser("~"), 'Desktop')

columns=[]
select=True
s=""
resultfile=[]
file_names=[]
file_col=[]
c=[]
allexcel=[]
allfile=[]
flag=True
leach =[]
barcode=""
def process_file(i):  
    global flag  
    global select  
    global leach  
    try:  
        # 优化  
        state = 0  
        
        for n in leach:  
            if n in i:  
                state = 1  
        """
        if '确认' in i:  
            state = 0  
        """
        df = read_csv_with_encoding(i)  
    except Exception as r:  
        print(i)  
        flag = False  
        print(r)  
    
    if state != 0:  
        try:  
            if flag:  
                if columns[0] in df.columns:  
                    print(i)  
                    if select:  
                        df = df.filter(pl.col(barcode).is_in(s))   
                    if len(df) != 0:  
                        file_name = i.split('\\')  
                        file_names.append(file_name)  
                        
                        if select:  
                            df = df.filter(pl.col(barcode).is_in(s))   
                        
                        # 使用 with_columns 进行赋值  
                        f = []  
                        k = 0  
                        for n in file_name:  
                            f.extend([n] * len(df))  # 创建一个长度与 df 一致的列表  
                        
                            df = df.with_columns(  
                                pl.Series(f).alias(file_col[k])  # 将列表作为新列  
                            )  
                            f = []  
                            k += 1  

                        for l in range(k, len(file_col)):  
                            df = df.with_columns(  
                                pl.lit(None).alias(file_col[l])  # 添加新的空列  
                            )  

                        # 选择结果  
                        global resultfile  
                        result = df.select([pl.col(col) for col in c if col in df.columns])  

                        # 如果需要填充不存在的列，可以添加新列，填充值为 null  
                        for col in c:  
                            if col not in df.columns:  
                                result = result.with_columns(pl.lit(None).alias(col))   
                        resultfile.append(result)  
        except Exception as r:  
            print(r)  
    
    flag = True


    
def process_files_in_parallel(file_paths):
    #results 一个迭代器
    global kernel_Num
    print(kernel_Num)
    with ThreadPoolExecutor(max_workers=kernel_Num) as executor:
        results = executor.map(process_file, file_paths)
    # 将每个 DataFrame 进行 Null 值填充和类型转换  
    resultfile_filled = []  
    
    for df in resultfile:  
     # 填充 Null 值为 ""  
        df_filled = df.fill_null("")  
        """
        # 打印填充后的 DataFrame 信息  
        print("填充后的 DataFrame 信息:")  
        print(df_filled)  
        """
        # 确保所有相同的列类型  
        for col in df_filled.columns:  
            # 如果列的类型是 Null，我们要转换为字符串类型  
            if df_filled[col].dtype == pl.Null:  
                df_filled = df_filled.with_columns(  
                    pl.col(col).cast(pl.Utf8)  
                )  
        
        resultfile_filled.append(df_filled)  

    file=get_desktop_path()+'/'+'合并结果.csv'
    if len(resultfile_filled )==0:
        print('NOT FOUND')
    elif len(resultfile_filled )==1:
        # 写入 CSV 文件  
        with open(file, "w", encoding="utf-8-sig") as f:  
            f.write(resultfile_filled [0].write_csv())  # 将 DataFrame 以 CSV 格式写入字符串，然后写入文件
    else:
        m=concat(resultfile_filled)
        with open(file, "w", encoding="utf-8-sig") as f:  
            f.write(m.write_csv())  

def run():
    global columns
    global select
    global s
    global resultfile
    global file_names
    global file_col
    global c
    global allexcel
    global allfile
    global flag
    global leach
    global barcode
    leach=e6.get().replace('\t',',').replace('\n',',').split(',')
    col=e1.get().replace('\t',',').replace('\n',',').split(',')
    columns=e2.get().replace('\t',',').replace('\n',',').split(',')
    select=True
    if e4.get()=='':
        select=False
    else:
        p_id=e4.get().replace('\n',',').replace('\t',',').split(',')
        if '' in p_id:
            p_id.remove('')
        barcode=e5.get().replace('\t',',').replace('\n',',').split(',')
        s=p_id
    if '' in col:
        col.remove('')
    if '' in columns:
        columns.remove('')
    file_col=['f1','f2','f3','f4','f5','f6','f7','f8','f9','f10'
              ,'f11','f12','f13','f14','f15','f16','f17','f18',
              'f19','f20','f21','f22','f23','f24','f25','f26',
              'f27','f28','f29','f30','f31','f32','f33','f34','f35']
    file_names=[]
    c=[]
    for i in file_col:
        c.append(i)
    for i in col:
        c.append(i)
    for i in columns:
        c.append(i)
    allPath=e3.get().replace('\t',',').replace('\n',',').split(',')
    
    #p_path=allPath
    allfile=[]
    #l=len(path.split('\\'))
    try:
        for patht in allPath:
            paths=os.walk(patht)
            for path, dir_lst, file_lst in paths:
                for file_name in file_lst:
                    allfile.append(os.path.join(path, file_name))
    except Exception as r:
        print(r)
    allexcel=[]

    flag=True
                          
    failfile=[]
                          
    resultfile=[]
    process_files_in_parallel(allfile)


tk.Button(root, text="开始查询并导出文件", width=20, command=run).grid(sticky='ws',row=6,column=0,padx=30, pady=5)  # W左边


root.mainloop()  # 进入消息循环
# 记录程序结束时间
end_time = time.time()

# 计算程序运行时间
run_time = end_time - start_time
print(f"程序运行时间：{run_time} 秒")

# 获取CPU使用率



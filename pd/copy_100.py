# 定义源文件和目标文件的文件名
source_file = 'C:/Users/Singer/Desktop/pd'
target_file = 'output.test'

# 打开源文件以读取内容
with open(source_file, 'r') as file:
    content = file.read()

# 打开目标文件以写入内容
with open(target_file, 'w') as file:
    # 将内容复制100遍并写入目标文件
    file.write(content * 100)
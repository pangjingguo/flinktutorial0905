```python
a = 1
a = 'aaaa'
1 + int('1') => 2
str(1) + '1' => '11'
```

定义函数：

```java
int add(int a, int b) {
    return a + b;
}
```

python写法

```python
def add(a, b):
    return a + b

add = lambda a, b : a + b

// 内部函数
// 闭包
def add(n):
    def helper(m):
        return m + n
    return helper
addOne = add(1)
addOne(2) => 3
addTwo = add(2)
addTwo(2) => 4
```

条件结构：

```python
if 2 > 1:
    print(2)
elif 1.5 > 1:
    print(1.5)
else:
    print(1)
```

循环结构：

```python
while True:
    pass

i = 0
while i < 10:
    print(i)
    i = i + 1
```

java

```java
for (int i = 0; i < 10; i++) {
    System.out.println(i);
}
```

python

```python
for i in range(0, 10):
    print(i)
```

面向对象

```python
class UserBehavior:
    def __init__(self, user_id, product_id, category_id, behavior_type, ts):
        self.user_id = user_id
        self.product_id = product_id
        self.category_id = category_id
        self.behavior_type = behavior_type
        self.ts = ts
        
    def get_user_id(self):
        return self.user_id
    
u = UserBehavior(1,2,3,'pv',1000)
print(u.user_id)
u.user_id = 10
print(u.get_user_id())
```

常用数据结构

list

```python
a = [1,2,3,4,5]
a.append(6) => [1,2,3,4,5,6]
a.pop() => [1,2,3,4,5]
a.pop(0) => [2,3,4,5]
```

列表推导式

```python
# 初始化一个10000个0组成的列表
a = [0 for i in range(0, 10000)]
```

遍历列表

```python
a = [1,2,3,4,5]
for i in a:
    print(i)
    
a = [i * i for i in a] => [1,4,9,16,25]

a = [1,2,3,4,5]
a = [i * i for i in a if i % 2 == 0] => [4, 16]

a = [1,2,3]
// 交换第0个和第2个元素
a[0], a[2] = a[2], a[0]

a = [['zuoyuan',2,3], ['baiyuan',5,6]]
map(lambda x: x[0], a) => ['zuoyuan','baiyuan']
```

字典

```python
// 初始化一个空字典
d = {}

// 初始化一个字典
dd = {
    'a': 1,
    'b': 2
}

// 写入kv键值对
d['name'] = 'zuoyuan'
d['age'] = 37

for k in d:
    print(k)
    print(d[k])
```

集合

```python
s = set()
s.add(1)
s.add(1)
s.remove(1)
```

元组

```python
t = (1, 'a')
t[0]
t[1]
```


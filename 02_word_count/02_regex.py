import re
WORD_RE = re.compile(r'[\w]+')

words = WORD_RE.findall('Big data, hadoop and map reduce. (Hello world!)', )
print(words)


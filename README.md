## The project of training chinese phi3-mini 

### Step 1. Collect and Clean Data
In the design language, this project aims to consider multiple text source, wikipedia, insgram, weibo etc.. I design a general
that can collect data and clean data architecture for considering asynchronous produce and consume process. The architecture
details please refer to ...

Because this project is a learning project for traning my ability to fullstack process in large scale data process and training llm, int this step I only
focus on collect and clean data from wikipedia.

There are many existing tools and packages for cleaning wikipedia text already, for example WikiExtractor package in python. However,
those packages only simplily discarded the template content `{{}}` and linkage `[[]]`, but it will discard many information such as
the language information with `lang` template, so I decide to rewrite the clean algorithm. If you are insterest in the implementation
details in this process, please refer to .... The algorithm may be rely on handy design of the template and link tag, if you find there
are many bug, please contact me.

### Step 2. Deduplication from the cleaned wikipedia text
Deduplication is very important, in this project, I apply minHash algorithm to deduplicate the data. The architecture and details
can refer to..
## Functional Requirements
### Wikitext clean
1. transfer wikitext into Markdown format
2. conserve significant information instead simply discard template and links.

### Deduplication
1. deduplicate the dataset
2. implements the LSH deduplicate algorithm

### Tokenize Server
deploying tokenizer as a server instead of the progress, because it can be used
not only in training process but also used in inferencing process. Which can reduce
the burden of the train and inference node.

import sqlparse
import re
import copy

op_keywords = {'SELECT': ['E']}
illegal_op_keywords = ['INSERT', 'DELETE', 'UPDATE', 'UPSERT', 'REPLACE', 'MERGE', 'DROP', 'CREATE', 'ALTER']
table_keywords = {'FROM':  ['E']}
cond_keyowrds = {'WHERE': {'next': ['E']}}
name_keywords = {'AS':  ['E']}
join_phrases = {'JOIN':  ['E'], 'INNER':  'JOIN', 'OUTER':  'JOIN', 'FULL':  'JOIN', 'LEFT':  'JOIN', 'RIGHT': 'JOIN', 'STRAIGHT_JOIN':  ['E']}
concat_keywords = {'AND':  ['E', ')'], 'OR':  ['E', ')'], '(': ['AND', 'OR', ')', 'E'], ')': ['E']}
join_cond_keywords = {'ON':  ['E']}
cond_op_keywords = {'IN':  ['E'], 'LIKE':  ['E'], '=':  ['E'], '>':  ['E', '='], '<':  ['E', '='], 'IS': ['NOT', 'NULL'], 'NOT': ['IN', 'NULL'], 'NULL': ['E']}

agg_keywords = {'MIN':  ['('], 'MAX':  ['('], 'COUNT':  ['('], 'SUM':  ['('], 'AVG': ['('], '(':  ['E', 'DISTINCT'], 'DISTINCT':  ['E']}
distinct_keywords = {"DISTINCT": ['E']}
group_keywords = {'BY':  ['E'], 'GROUP':  'BY'}
order_keywords = {'BY':  ['E'],'ORDER':  'BY', 'DESC': ['E']}

Wildcard = {'*': 'all'}

ops = {'query': {'op': op_keywords, 'agg': agg_keywords}, 
       'table': {'table_cond': table_keywords}, 
       'filter': {'op': cond_keyowrds, 'filter_cond': cond_op_keywords}, 
       'table_join': {'op': join_cond_keywords, 'join_cond': join_cond_keywords},
       'group': {'group_cond': group_keywords},
       'order': {'order_cond': order_keywords}}

keywords_op_map = {'SELECT': 'query', 'FROM': 'table', 'WHERE': 'filter', 'AND':  'filter', 'OR':  'filter', 'ON': 'table_join', 'GROUP': 'group', 'ORDER': 'order', 
                   'JOIN':  'table_join', 'INNER': 'table_join', 'OUTER':'table_join', 'FULL': 'table_join', 'LEFT': 'table_join', 'RIGHT': 'table_join', 'STRAIGHT_JOIN': 'table_join',
                   'EXCEPT': 'split', 'UNION': 'split', 'INTERSECT': 'split'}

ILLEGAL_OPERATION = "Illegal Operation"
WRONG_FORMAT = "Wrong format"
RIGHT_FORMAT = "Yes, format is right" 


def get_op_type(line, first_kw):
    if first_kw in illegal_op_keywords:
        return ILLEGAL_OPERATION, ''
    if line.startswith('  '):
        return RIGHT_FORMAT, ''
    if first_kw not in keywords_op_map:
        return WRONG_FORMAT, ''
    else:
        return RIGHT_FORMAT, keywords_op_map[first_kw]

def merged_words(words):
    new_words = []
    sheild_keyword = False
    merge_str = ""
    for word in words:
        if len(re.findall(r'\"', word))%2!=0 or len(re.findall(r"\'", word))%2!=0:
            sheild_keyword = not sheild_keyword

        if sheild_keyword:
            merge_str += word
            merge_str += ' '
        elif merge_str != "":
            merge_str += word
            new_words.append(merge_str)
            merge_str = ''
        else:
            new_words.append(word)

    return new_words


def factor_processing(factor):
    factor = factor.replace('(', ' ( ').replace(')', ' ) ')
    sub_factors = factor.split(' ')
    while '' in sub_factors:
        sub_factors.remove('')
    sub_factors = merged_words(sub_factors)
    
    
    return sub_factors
    
def parsing_state_machine(factor, type_keywords):
    sub_factors = factor_processing(factor)
    # print(sub_factors)
    keywords = []
    values = []

    for si in range(len(sub_factors)):
        sub_factor = sub_factors[si]
        if sub_factor.upper() in type_keywords:
            sub_factor = sub_factor.upper()
            if si == (len(sub_factors)-1):
                if sub_factor != "DESC" and sub_factor != ")" :
                    return WRONG_FORMAT, '', []
            elif sub_factors[si+1] not in type_keywords[sub_factor] and 'E' not in type_keywords[sub_factor]:
                return WRONG_FORMAT, '', []
            keywords.append(sub_factor) 
        else:
            if sub_factor != ')':
                values.append(sub_factor)
    if len(keywords)> 0 and keywords[0] in ('<', '>'):
        keyword = "".join(keywords)
    else:
        keyword = " ".join(keywords)
    
    return RIGHT_FORMAT, keyword, values

def sel_parsing(words):
    factors = " ".join(words).split(",")
    while '' in factors:
        factors.remove('')
    sels = []
    agg = {}
    distinct_ids = []
    vi = 0
    for factor in factors:
        format, keyword, values = parsing_state_machine(factor, agg_keywords)
        if format == WRONG_FORMAT:
            return WRONG_FORMAT, {}
        if keyword != '':
            agg[values[0]]= keyword.split(' ')[0]
        if keyword.endswith('DISTINCT'):
            distinct_ids.append(vi)
        if len(values) > 1 and 'AS' in values:
            column_factors = " ".join(values).split('AS')
            sels.append((column_factors[0], column_factors[1]))    
        else:
            sels.append((values[0], ))
        vi += 1
    parsed_map = {'sel': sels}
    if len(agg) > 0:
        parsed_map['agg'] = agg
    if len(distinct_ids) > 0:
        parsed_map['distinct'] = distinct_ids
        
    return RIGHT_FORMAT, parsed_map

def table_parsing(words):
    if len(words) == 3 and 'AS' in words:
        table = (words[0], words[2])
    elif len(words) == 1:
        table = (words[0], )
    else:
        return WRONG_FORMAT, {}
    parsed_map = {'table': table}
    return RIGHT_FORMAT, parsed_map

def single_cond_parsing(cond):
    cond_items = re.sub(r"|".join(cond_op_keywords.keys()), cond)
    while '' in cond_items:
        cond_items.remove('')
    if len(cond_items) != 2:
        return WRONG_FORMAT, {}
    col = cond_items[0]
    cond_op = cond[len(cond_items[0]): (0-len(cond_items[1]))].strip()
    value = cond_items[1]
    
    return (col, cond_op, value)
    
def filter_parsing(words):
    while '' in words:
        words.remove('')
    factor = "#".join(words)
    # print(factor)
    factor = re.sub("^AND#|#AND#", " AND ", factor.strip())
    factor = re.sub("^OR#|#OR#", " OR ", factor.strip())
    format, concat_keyword, cond_strs = parsing_state_machine(factor, concat_keywords)
    if format == WRONG_FORMAT:
        return WRONG_FORMAT, {}
    conds = []
    for cond in cond_strs:
       # print(cond)
        cond = cond.replace("#", " ")
        for cond_op_kw in cond_op_keywords:
           cond = cond.replace(cond_op_kw, ' ' + cond_op_kw + ' ')
        format, cond_keyword, cond_values = parsing_state_machine(cond, cond_op_keywords)
        # print(format, cond_keyword, cond_values)
        if format == WRONG_FORMAT or len(cond_values) < 2:
            return WRONG_FORMAT, {}
        conds.append((cond_values[0], cond_keyword, " ".join(cond_values[1:])))
        # conds.append(single_cond_parsing(cond))
    concat_keyword = concat_keyword.split(' ')
    while '' in concat_keyword:
        concat_keyword.remove('')
    parsed_map = {'cond_concat': concat_keyword, 'conds': conds}

    return RIGHT_FORMAT, parsed_map

def group_parsing(words):
    factors = " ".join(words).split(",")
    while '' in factors:
        factors.remove('')
    groupBy = []
    vi = 0
    for factor in factors:
        format, keyword, values = parsing_state_machine(factor, group_keywords)
        if keyword != 'BY':
            return WRONG_FORMAT, {}
        groupBy.append(values[0])
    parsed_map = {"groupBy": groupBy}
    
    return RIGHT_FORMAT, parsed_map


def order_parsing(words):
    factors = " ".join(words).split(",")
    while '' in factors:
        factors.remove('')
    orderBy, desc_cols = [], set()
    for factor in factors:
        format, keyword, values = parsing_state_machine(factor, order_keywords)
        if (keyword != 'BY' and keyword != 'BY DESC') or format == WRONG_FORMAT:
            return WRONG_FORMAT, {}
        orderBy.append(values[0])
        if 'DESC'in keyword:
            desc_cols.add(values[0])
        
    parsed_map = {"orderBy": orderBy, "desc": desc_cols}
    
    return RIGHT_FORMAT, parsed_map


def table_join_parsing(words):
    while '' in words:
        words.remove('')
    factor = " ".join(words)
    format, join_keyword, values = parsing_state_machine(factor, join_phrases)
    value_factors = " ".join(values).split(" ON ")  # join_table AS named_table ON cond
    format, join_table_map = table_parsing(value_factors[0].split(" "))
    if format == WRONG_FORMAT:
        return WRONG_FORMAT, {}
    format, join_cond_map = filter_parsing(value_factors[1].split(" "))
    # print(join_cond_map)
    if format == WRONG_FORMAT:
        return WRONG_FORMAT, {}

    parsed_map = {'join_type': [join_keyword], 'join_table': [join_table_map["table"]]}

    if join_cond_map['cond_concat'] != [] and len(join_cond_map['conds']) < 2:
        return WRONG_FORMAT, parsed_map
    
    parsed_map['join_cond_concat'] = join_cond_map['cond_concat']
    parsed_map['join_conds'] = join_cond_map['conds']
    
    return format, parsed_map


def parsed_map_merge(old_map, new_sub_map):
    for k, v in new_sub_map.items():
        if k not in old_map:
            old_map[k] = v
        else:
            old_map[k].extend(v)
            
    return old_map


parsing_funs = {'query': sel_parsing, 
                'table': table_parsing, 
                'filter': filter_parsing, 
                'table_join': table_join_parsing,
                'group': group_parsing,
                'order': order_parsing}

def sql_parsing(query):
    query_format = sqlparse.format(query, reindent=True, keyword_case='upper')
    lines = query_format.split("\n")
    parsed_map = {}
    current_op_type = ''
    parsed_map_list = []
    for li in range(len(lines)):
        line = lines[li]
        # print(line)
        words = line.split(" ")
        format, op_type = get_op_type(line, words[0])
        if format == ILLEGAL_OPERATION:
            return WRONG_FORMAT, parsed_map
        if (li == 0 and op_type != 'query') or format == WRONG_FORMAT:
            return WRONG_FORMAT, parsed_map
        # print(op_type)
        if op_type == 'split':
            old_parsed_map = copy.deepcopy(parsed_map)
            # print(old_parsed_map)
            parsed_map_list.append(old_parsed_map)
            parsed_map = {}
            continue
        if op_type != '':
            current_op_type = op_type
            if current_op_type != 'table_join':
                words = words[1:]
                
        parsing_fun = parsing_funs[current_op_type]
        format, sub_parsed_map = parsing_fun(words)
        parsed_map = parsed_map_merge(parsed_map, sub_parsed_map)

        if format == WRONG_FORMAT:
            if len(parsed_map) > 0:
                parsed_map_list.append(parsed_map)
            return WRONG_FORMAT, parsed_map_list
    if len(parsed_map) > 0:
        parsed_map_list.append(parsed_map)
    return RIGHT_FORMAT, parsed_map_list

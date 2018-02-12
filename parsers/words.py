import pandas as pd
import nltk
from nltk.tokenize import RegexpTokenizer

def calculate_words(element, result):
    results = element.split(' ')
    for key in results:
        if key in result:
            result[key] += 1
        else:
            result[key] = 1
    return result

def nltk_calculate_words(series: pd.Series, top = 100):
    string = series.str.lower().str.cat(sep=' ')
    tokenizer = RegexpTokenizer(r'\w+')
    words = tokenizer.tokenize(string)
    word_dist = nltk.FreqDist(words)
    return pd.DataFrame(word_dist.most_common(top), columns=['Word', 'Frequency'])


def nltk_word_frequencies(series: pd.Series, words=None, freq="15T"):
    if words is None:
        words = []

    def calc(df, column_name):
        string = df[column_name].str.lower().str.cat(sep=' ')
        tokenizer = RegexpTokenizer(r'\w+')
        words_distribution = nltk.FreqDist(tokenizer.tokenize(string))
        result = pd.Series()
        for word in words:
            result[word] = words_distribution[word]
        return result

    rv = series.groupby(pd.Grouper(freq=freq)).apply(calc, 'text')
    norm = series['id'].groupby(pd.Grouper(freq=freq)).count()
    for word in words:
        rv[word] = rv[word] / norm
        rv[word] = rv[word] / rv[word].max()
    return rv
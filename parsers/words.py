import pandas as pd
import nltk

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
    words = nltk.tokenize.word_tokenize(string)
    word_dist = nltk.FreqDist(words)
    return pd.DataFrame(word_dist.most_common(top), columns=['Word', 'Frequency'])

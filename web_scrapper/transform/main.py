import argparse
import logging
logging.basicConfig(level = logging.INFO)
from  urllib.parse import urlparse 
import pandas as pd
import hashlib
import nltk
from nltk.corpus import stopwords

logger = logging.getLogger(__name__)



def main(filename):
    logger.info('Starting cleaning process')
    df = _read_data(filename)
    newspaper_uid = _extract_newspaper_uid(filename)
    df = _add_newspaper_uid_column(df,newspaper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)
    df = _generate_uids_for_row(df)
    df = _remove_new_lines(df, 'body')
    df = _remove_new_lines(df, 'title')
    df = _tokenize_columns(df, 'title')
    df = _tokenize_columns(df, 'body')
    df = _remove_duplicate_values(df, 'title')
    df = _drop_rows_with_missing_values(df)
    _save_data(df, filename)
    return df

def _read_data(filename):
    logger.info('Reading file {}'.format(filename))
    return pd.read_csv(filename)

def _extract_newspaper_uid(filename):
    logger.info('Extrating newspaper uid')
    newspaper_uid = filename.split('_')[0]
    logger.info('Newspaper uid detected: {}'.format(newspaper_uid))
    return newspaper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('Filling newspaper_uid column with {}'.format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid
    return df

def _extract_host(df):
    logger.info('Extrating host form urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df

def _fill_missing_titles(df):
    logger.info('Filling missing titltes')
    missing_titles_mask = df['title'].isna()

    missing_titles = (df[missing_titles_mask]['url']
                        .str.extract(r'(?P<missing_titles>[^/]+)$')
                        .applymap(lambda title: title.split('-'))
                        .applymap(lambda title_word_list: ' '.join(title_word_list))
                        )
    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
    return df

def _generate_uids_for_row(df):
    logger.info('Generating uids for each row')
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'], 'utf-8')), axis = 1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )
    df['uid'] = uids
    return df.set_index('uid')

def _remove_new_lines(df, column):
    logger.info('Remove new line, line break and unnecesary characters')   
    stripped_body = (df
                    .apply(lambda row: row[column], axis =1)
                    .apply(lambda body: list(body))
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\n',''), letters)))
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\t',''), letters)))
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\r',''), letters)))
                    .apply(lambda letters: list(map(lambda letter: letter.replace('0',''), letters)))
                    .apply(lambda letters: list(map(lambda letter: letter.replace('%',''), letters)))
                    .apply(lambda letters:''.join(letters))
                    )
    df[column] = stripped_body
    return df

def _tokenize_columns(df, column):
    logger.info('Calculating the number of unique tokens in {}'.format(column))
    stop_words = set(stopwords.words('spanish'))
    tokenizer = (df
                    .dropna()
                    .apply(lambda row: nltk.word_tokenize(row[column]), axis =1)
                    .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
                    .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
                    .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
                    .apply(lambda valid_word_list: len(valid_word_list))
                )
    df[str('n_token_{}'.format(column))] = tokenizer
    return df

def _remove_duplicate_values(df, column_name):
    logger.info('Removing duplicate entries')
    df.drop_duplicates(subset = [column_name], keep = 'first', inplace = True)
    return df


def _drop_rows_with_missing_values(df):
    logger.info('Dropping rows with missinf values')
    return df.dropna()

def _save_data(df, filename):
    clean_filename = 'clean_{}'.format(filename)
    logger.info('Saving the data at {}'.format(clean_filename))
    df.to_csv(clean_filename)




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help = 'The path to the dirty data',
                        type=str)
    arg = parser.parse_args()
    df = main(arg.filename)
    print(df)
import re


def phone_extract(post):

    # define what punctuation is
    # punct = '!'#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'

    # exclude $ from punctation, as sometimes a price is listed after phone
    # number and I don't want to lump together
    punct = '!'#%&\'()*+,-./:;<=>?@[\\]^_`{|}~'

    # zap the post into lowercase
    post = post.lower()

    # remove all punctation
    post = ''.join(l for l in post if l not in punct)

    # remove all spaces
    post = post.replace(' ', '')

    # create a dict of numeric words to replace with numbers
    numbers = {'zero': '0',
               'one': '1',
               'two': '2',
               'three': '3',
               'four': '4',
               'five': '5',
               'six': '6',
               'seven': '7',
               'eight': '8',
               'nine': '9'}

    # look for each number spelled out in the post, and if found, replace with
    # the numeric alternative
    for num in numbers:
        if num in post:
            post = post.replace(num, numbers[num])

    # extract all number sequences
    numbers = re.findall('\d+', post)

    # filter number strings to only include unique strings longer that are
    # betweeb 7 and 11 characters in length
    phones = set([i for i in numbers if len(i) >= 7 and len(i) <= 11])

    # convert set to semicolon delimited for easy formatting in CSV
    if len(phones) > 0:
        phone_del = ';'.join([i for i in phones])

    # if there are no phones found, define N/A
    else:
        phone_del = 'N/A'

    return phone_del

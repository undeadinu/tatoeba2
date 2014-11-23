from tatoeba2.models import Sentences, SentenceComments, SentencesTranslations, Users, TagsSentences, SentencesSentencesLists, FavoritesUsers, SentenceAnnotations, Contributions, Wall
from datetime import datetime
from tatoeba2.management.commands.deduplicate import Dedup
from django.db import connections
from django.db.models.loading import get_model
import pytest
import os


def pytest_addoption(parser):
    parser.addoption(
    '--mysql', action='store_true',
    help='handles mysql-specific resets that even transactions can\'t roll back...(gg mysql, gg)'
    )


@pytest.fixture
def sents(db, request):

    # no owner, no audio, no correctness 1-4
    Sentences(text='Normal, not duplicated.', lang='eng', created=datetime(2014, 1, 1)).save()
    for i in xrange(3): Sentences(text='Normal, duplicated.', lang='eng', created=datetime(2014, 1, 1)).save()

    # has owner 5-8
    Sentences(text='Has owner, not duplicated.', lang='eng', user_id=1, created=datetime(2014, 1, 2)).save()
    for i in xrange(2): Sentences(text='Has owner, duplicated.', lang='eng', created=datetime(2014, 1, 2)).save()
    Sentences(text='Has owner, duplicated.', lang='eng', user_id=1, created=datetime(2014, 1, 2)).save()

    # has audio 9-12
    Sentences(text='Has audio, not duplicated.', lang='eng', hasaudio='shtooka', created=datetime(2014, 1, 3)).save()
    for i in xrange(2): Sentences(text='Has audio, duplicated.', lang='eng', created=datetime(2014, 1, 3)).save()
    Sentences(text='Has audio, duplicated.', lang='eng', hasaudio='shtooka', created=datetime(2014, 1, 3)).save()

    # correctness -1  13-16
    Sentences(text='Correctness -1, not duplicated.', lang='eng', correctness=-1, created=datetime(2014, 1, 4)).save()
    for i in xrange(2): Sentences(text='Correctness -1, duplicated.', lang='eng', created=datetime(2014, 1, 4)).save()
    Sentences(text='Correctness -1, duplicated.', lang='eng', correctness=-1, created=datetime(2014, 1, 4)).save()
    
    # has owner, has audio, correctness -1  17-21
    Sentences(text='Has owner, Has audio, Correctness -1, not duplicated.', lang='eng', user_id=1, hasaudio='shtooka', correctness=-1, created=datetime(2014, 1, 5)).save()
    Sentences(text='Has owner, Has audio, Correctness -1 duplicated.', lang='eng', created=datetime(2014, 1, 5)).save()
    Sentences(text='Has owner, Has audio, Correctness -1 duplicated.', lang='eng', user_id=1, created=datetime(2014, 1, 5)).save()
    Sentences(text='Has owner, Has audio, Correctness -1 duplicated.', lang='eng', hasaudio='shtooka', created=datetime(2014, 1, 5)).save()
    Sentences(text='Has owner, Has audio, Correctness -1 duplicated.', lang='eng', correctness=-1, created=datetime(2014, 1, 5)).save()
    
    for i in xrange(6, 8+1): SentenceComments(sentence_id=i, text='Comment on '+str(i), user_id=1, created=datetime.now(), hidden=0).save()
    
    SentencesTranslations(sentence_id=6, translation_id=9, distance=1).save()
    SentencesTranslations(sentence_id=9, translation_id=6, distance=1).save()
    SentencesTranslations(sentence_id=7, translation_id=10, distance=1).save()
    SentencesTranslations(sentence_id=10, translation_id=7, distance=1).save()
    
    TagsSentences(tag_id=1, sentence_id=6, user_id=1, added_time=datetime.now()).save()
    TagsSentences(tag_id=2, sentence_id=7, user_id=1, added_time=datetime.now()).save()
    TagsSentences(tag_id=3, sentence_id=8, user_id=1, added_time=datetime.now()).save()
    
    SentencesSentencesLists(sentences_list_id=1, sentence_id=6).save()
    SentencesSentencesLists(sentences_list_id=2, sentence_id=7).save()
    SentencesSentencesLists(sentences_list_id=3, sentence_id=8).save()
    
    FavoritesUsers(user_id=1, favorite_id=6).save()
    FavoritesUsers(user_id=2, favorite_id=7).save()
    FavoritesUsers(user_id=3, favorite_id=8).save()
    
    SentenceAnnotations(meaning_id=1, text='', modified=datetime.now(), user_id=1, sentence_id=6).save()
    SentenceAnnotations(meaning_id=2, text='', modified=datetime.now(), user_id=1, sentence_id=7).save()
    SentenceAnnotations(meaning_id=3, text='', modified=datetime.now(), user_id=1, sentence_id=8).save()
    
    Contributions(text='Logs for 6', action='update', user_id=1, datetime=datetime.now(), type='sentence', sentence_id=6).save()
    Contributions(text='Logs for 6', action='insert', user_id=1, datetime=datetime.now(), type='link', sentence_id=6, translation_id=9).save()
    Contributions(text='Logs for 7', action='insert', user_id=1, datetime=datetime.now(), type='sentence', sentence_id=7).save()
    Contributions(text='', action='insert', user_id=1, datetime=datetime.now(), type='sentence', sentence_id=8).save()
    Contributions(text='Unknown datetime record', action='update', user_id=1, datetime=None, type='sentence', sentence_id=8).save()

    Wall(owner=1, content='test post', date=datetime.utcnow(), title='', hidden=0, lft=1, rght=2).save()

    if request.config.option.mysql:
        def fin():
            conn = connections['default']

            def clean_up(model):
                Model = get_model('tatoeba2.'+model)
                Model.objects.all().delete()
                conn.cursor().execute('TRUNCATE TABLE '+Model._meta.db_table+';')
                conn.cursor().execute('ALTER TABLE '+Model._meta.db_table+' AUTO_INCREMENT = 1;')

            clean_up('Sentences')
            clean_up('SentencesTranslations')
            clean_up('SentenceComments')
            clean_up('TagsSentences')
            clean_up('SentencesSentencesLists')
            clean_up('FavoritesUsers')
            clean_up('Contributions')
            clean_up('Users')
            clean_up('Wall')
            clean_up('SentenceAnnotations')

        request.addfinalizer(fin)

@pytest.fixture
def bot(db):
    return Users.objects.create(
                username='deduplication_bot', password='', email='bot@bots.com',
                since=datetime.now(), last_time_active=datetime.now().strftime('%Y%m%d'),
                level=1, is_public=1, send_notifications=0, group_id=1
                )

@pytest.fixture
def dedup(request, bot):
    Dedup.time_init()
    Dedup.logger_init()
    Dedup.dry = False
    Dedup.bot = bot
    
    def fin():
        os.remove(Dedup.log_file_path)
    request.addfinalizer(fin)
    
    return Dedup

@pytest.fixture
def linked_dups():
    SentencesTranslations(sentence_id=2, translation_id=1, distance=1).save()
    SentencesTranslations(sentence_id=3, translation_id=1, distance=1).save()
    SentencesTranslations(sentence_id=3, translation_id=2, distance=1).save()
    SentencesTranslations(sentence_id=2, translation_id=3, distance=1).save()

@pytest.fixture
def dups_in_list():
    SentencesSentencesLists(sentences_list_id=4, sentence_id=2).save()
    SentencesSentencesLists(sentences_list_id=4, sentence_id=3).save()

@pytest.fixture
def dups_in_fav():
    FavoritesUsers(user_id=1, favorite_id=2).save()
    FavoritesUsers(user_id=1, favorite_id=3).save()
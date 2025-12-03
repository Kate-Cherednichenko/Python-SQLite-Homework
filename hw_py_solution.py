import tokenize_uk as tok
import pymorphy2
import sqlite3
from sqlite3 import Error
import os


class Word:
    s_morph = pymorphy2.MorphAnalyzer(lang='uk')
    
    def __init__(self, form, lemma, pos, freq):
        self._form = form
        self._lemma = lemma
        self._pos = pos
        self._freq = freq

    @staticmethod
    def from_token(token, freq):
        parsed = Word.s_morph.parse(token)[0]
        pos = parsed.tag.POS if parsed.tag.POS else 'UNKNOWN'
        return Word(token, parsed.normal_form, pos, freq)

    def show(self):
        print(f"Словоформа: {self._form:<15} Лема: {self._lemma:<15} Частина мови: {self._pos:<10} Частота: {self._freq}")

    def to_tuple(self):
        return (self._form, self._lemma, self._pos, self._freq)

class Sample:
    def __init__(self, text):
        self._text = text
        self._tokens = tok.tokenize_words(self._text)
        # Залишаємо тільки слова, що починаються з літери
        self._words_list = [s.lower() for s in self._tokens if s and s[0].isalpha()]

    def get_words(self):
        words_set = set()
        words = []
        for word_str in self._words_list:
            if word_str in words_set:
                continue
            
            words_set.add(word_str)
            freq = self._words_list.count(word_str)
            words.append(Word.from_token(word_str, freq))

        return words

    def show(self, words):
        for word in words:
            word.show()

class SQL:
    def __init__(self, db_file):
        self._connection = SQL.create_connection(db_file)
        if self._connection:
            self._cursor = self._connection.cursor()

    @staticmethod
    def create_connection(db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            print(f"Connected to SQLite: {sqlite3.version}")
        except Error as e:
            print(e)
        return conn

    def create_word_freq_table(self):
        # Drop table if exists to start clean for HW purposes
        self._cursor.execute("DROP TABLE IF EXISTS WordFreq")
        
        create_word_freq = '''
        CREATE TABLE WordFreq (
            word_id INTEGER PRIMARY KEY AUTOINCREMENT,
            form TEXT,
            lemma TEXT,
            pos TEXT,
            freq INTEGER
        );
        '''
        try:
            self._cursor.execute(create_word_freq)
            self._connection.commit()
            print("Таблицю WordFreq створено успішно.")
        except Error as e:
            print(f"Помилка створення таблиці: {e}")

    def insert_into_freq_table(self, word):
        data_tuple = word.to_tuple()
        query = '''
        INSERT INTO WordFreq (form, lemma, pos, freq)
        VALUES(?, ?, ?, ?);
        '''
        self._cursor.execute(query, data_tuple)

    def generate_word_freq_table(self, words):
        self.create_word_freq_table()
        for word in words:
            self.insert_into_freq_table(word)
        self._connection.commit()
        print(f"Вставлено {len(words)} слів у базу даних.")

    def select_all(self):
        print("\n--- Всі записи в базі ---")
        rows = self._cursor.execute("SELECT * FROM WordFreq")
        for row in rows:
            print(row)

    def select_word_by_form(self, form):
        print(f"\n--- Пошук слова '{form}' ---")
        query = "SELECT * FROM WordFreq WHERE form = ?"
        self._cursor.execute(query, (form,))
        rows = self._cursor.fetchall()
        for row in rows:
            print(row)
        if not rows:
            print("Слово не знайдено.")

    def select_pos_freq(self):
        print("\n--- Статистика по частинах мови ---")
        query = '''
        SELECT pos, COUNT(word_id)
        FROM WordFreq
        GROUP BY pos
        '''
        rows = self._cursor.execute(query)
        for row in rows:
            print(f"POS: {row[0]}, Count: {row[1]}")

    def update_word_freq(self, form, new_freq):
        print(f"\n--- Оновлення частоти для слова '{form}' на {new_freq} ---")
        query = '''
        UPDATE WordFreq
        SET freq = ?
        WHERE form = ?
        '''
        try:
            self._cursor.execute(query, (new_freq, form))
            self._connection.commit()
            if self._cursor.rowcount > 0:
                print("Успішно оновлено.")
            else:
                print("Слово для оновлення не знайдено.")
        except Error as e:
            print(f"Помилка оновлення: {e}")

    def close(self):
        if self._connection:
            self._connection.close()



if __name__ == "__main__":
    sample_text = """
    Надворі дощ і сильний вітер. 
    Небо затягнуте хмарами, а на вулиці йде сильний дощ. 
    Це гарна погода для того, щоб залишитися вдома з чашкою гарячого чаю.
    """
    
    db_name = "hw_solution.db"
    
    print("КРОК 1: Аналіз тексту")
    smpl = Sample(sample_text)
    words = smpl.get_words()

    print("\nКРОК 2: Створення БД та наповнення")
    sql = SQL(db_name)
    sql.generate_word_freq_table(words)

    print("\nКРОК 3: Виконання SELECT запитів")
    sql.select_pos_freq()
    
    target_word = "дощ"
    sql.select_word_by_form(target_word)

    print("\nКРОК 4: Оновлення записів (UPDATE)")
    sql.update_word_freq(target_word, 100)

    print("\nКРОК 5: Перевірка змін")
    sql.select_word_by_form(target_word)
    
        sql.close()
    print("\nЗавдання виконано успішно.")
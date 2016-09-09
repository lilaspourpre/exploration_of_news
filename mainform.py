# -*- coding: utf-8 -*-'
from matplotlib import rc
rc('font',**{'family':'verdana'})
try:
    import matplotlib.pyplot as plt
except:
    raise
from PyQt4 import QtCore, QtGui, uic
import request_parser, psycopg2, datetime, networkx as nx, codecs,os.path
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt4agg import (
    FigureCanvasQTAgg as FigureCanvas,)
class MainForm(QtGui.QMainWindow):
    # конструктор
    def __init__(self, app):
        super(MainForm, self).__init__()
        # динамически загружает визуальное представление формы
        self.app = app # QtApplication для смены языка
        self.lang = 'en'
        uic.loadUi("first.ui", self) #загружает UIfile QtDesigner, интерфейс программы
        self.setWindowTitle(self.app.translate('MainWindow','Query Window')) #заголовок главного окна
        self.lineEdit.setPlaceholderText(self.app.translate('MainWindow','Write your query here')) #текст незаполненной строки ввода
        self.text = ['',''] #форма для открытия окна загрузки статьи вида: [articleName, language]
        self.pushButton4.setDisabled(True) #кнопка "построить граф" неактивна до соответствующего запроса"
        self.pushButton8.clicked.connect(self.openDialog) #открытие диалогового окна при нажатии кнопки "Upload"
        self.pushButton4.clicked.connect(self.openGraph) #построение графа при нажатии кнопки "Build Graph"
        self.pushButton_ct.clicked.connect(self.del_table) #очистка таблицы при нажатии кнопки "Clear table"
        self.pushButton_2.clicked.connect(self.fixText) #фиксация текста, обработка запроса при нажатии кнопки "Enter"
        self.lineEdit.returnPressed.connect(self.fixText) #фиксация текста, обработка запроса при нажатии клавиши "Enter" на клавиатуре
        self.pushButton_cl.clicked.connect(self.listWidget.clear) #очистка истории запросов при нажатии кнопки "Clear history"
        self.listWidget.itemDoubleClicked.connect(self.choose_text) #реализация запроса, хранящегося в истории запроса
        self.pushButton_3.clicked.connect(self.openHelp) #вызов справки при нажатии кнопки "Help"
        #возможность отмечать флагом выбранный язык
        self.actionEnglish.setCheckable(True)
        self.actionRussian.setCheckable(True)
        self.actionFrench.setCheckable(True)
        self.actionEnglish.setChecked(True) #английский язык - язык интерфейса по умолчанию
        #смена языка при выборе языка в контекстном меню (R = Russian, E = English, F = French)
        self.actionRussian.triggered.connect(self.changeLangR)
        self.actionEnglish.triggered.connect(self.changeLangE)
        self.actionFrench.triggered.connect(self.changeLangF)

    def changeLangR(self):
        self.actionRussian.setChecked(True) #выбран русский язык, остальные не выбраны
        self.actionEnglish.setChecked(False)
        self.actionFrench.setChecked(False)
        self.changeLang('translation/ru_translation.qm') #установление языка

    def changeLangE(self):
        self.actionRussian.setChecked(False)
        self.actionEnglish.setChecked(True) #выбран английский язык, остальные не выбраны
        self.actionFrench.setChecked(False)
        self.changeLang('en') #установление языка

    def changeLangF(self):
        self.actionRussian.setChecked(False)
        self.actionEnglish.setChecked(False)
        self.actionFrench.setChecked(True) #выбран французский язык, остальные не выбраны
        self.changeLang('translation/fr_translation.qm') #установление языка

    def changeLang(self, lang):
        self.lang=lang
        try:
            self.app.removeTranslator(self.translator) #модель перевода удаляется
            self.app.removeTranslator(self.common_translator)
        except:
            pass
        if lang != 'en': #если выбран не английский
            self.translator = QtCore.QTranslator() #вызывается переводчик
            self.translator.load(lang) #загружается перевод
            self.app.installTranslator(self.translator) #переводчик устанавливается в приложение

            self.common_translator = QtCore.QTranslator() #вызывается переводчик
            if 'fr_' in lang:
                self.lang='fr'
                load = 'translation/qt_fr.qm' #загружается перевод
            elif 'ru_' in lang:
                self.lang='ru'
                load = 'translation/qt_ru.qm'
            self.common_translator.load(load) #загружается перевод
            self.app.installTranslator(self.common_translator) #переводчик устанавливается в приложение
        #обновляются все символьные виджеты
        self.setWindowTitle(self.app.translate('MainWindow','Query Window')) #заголовок главного окна
        self.lineEdit.setPlaceholderText(self.app.translate('MainWindow','Write your query here')) #текст незаполненной строки ввода
        self.label.setText(self.app.translate('MainWindow','<html><head/><body><p align="center"><span style=" font-size:11pt;">Previous queries</span></p></body></html>'))
        self.label_2.setText(self.app.translate('MainWindow','<html><head/><body><p align="center"><span style=" font-size:11pt;">Table representation</span></p></body></html>'))
        self.pushButton_cl.setText(self.app.translate('MainWindow','Clear history'))
        self.pushButton_ct.setText(self.app.translate('MainWindow','Clear table'))
        self.pushButton4.setText(self.app.translate('MainWindow','Build graph'))
        self.pushButton_3.setText(self.app.translate('MainWindow','Help'))
        self.pushButton8.setText(self.app.translate('MainWindow','Upload article'))
        self.pushButton_2.setText(self.app.translate('MainWindow','Enter'))
        self.actionEnglish.setText(self.app.translate('MainWindow','English'))
        self.actionRussian.setText(self.app.translate('MainWindow','Russian'))
        self.actionFrench.setText(self.app.translate('MainWindow','French'))
        self.menuMenu.setTitle(self.app.translate('MainWindow','Menu'))
        self.menuChange_language.setTitle(self.app.translate('MainWindow','Change language'))

    def choose_text(self, item):
        self.request(unicode(item.text())) #выполняем запрос, отправляя текст, располагающийся в истории запросов
    def del_table(self):
        " удаляем таблицу из окна просмотра таблиц "
        table_model = MyTableModel([[]], [],self) #формируем пустую модель
        self.tableView.setModel(table_model) #устанавливаем пустую модель
    def fixText(self):
        "фиксация текса со строки запроса"
        text = unicode(self.lineEdit.text())
        self.request(text) #посылаем запрос в БД
        self.listWidget.addItem(text) #добавляем текст запроса в историю запросов
        self.lineEdit.clear() #очищаем строку запроса

    def request(self, text):
        RP = request_parser.RequestParser() #вызов парсера для формирования SQL-запроса
        try:
            RP.connect() #установка соединения
            if 'upload' in text: #загружаем статью - открываем диалоговое окно загрузки
                if ' ru' in text or ' en' in text:
                    self.upload_parser(text.replace('upload ',''))
                else:
                    res_decoded, result = [['Verify the language']], [[],['Error']]
            else: #или передаем запрос для формирования SQL-запроса
                result = RP.request_parser(text) #результат SQL-запроса в виде списков возвращается в переменную
                if result[0] != []: #если ответ БД не пустой
                    if len(result)==3: #если запрос был относительно связей для построения графа (дополнительный item = словарь для графа)
                        self.pushButton4.setDisabled(False) #кнопка "Построить граф" становится активной
                        self.openGraphPrinter = GraphForm(self, req=text, clusttexts=result[2], app=self.app)  #формируем конструктор окна
                        res_decoded = [(i[0],i[1].decode('utf-8')) for i in result[0]] #decode для отображения русского текста
                    else:
                        try:
                            res_decoded = [(i[0],i[1].decode('utf-8')) for i in result[0]] #decode для отображения русского текста
                        except:
                            res_decoded = result[0]
                        finally:
                            self.pushButton4.setDisabled(True) #кнопка неактивна, если запрос не по графам
                else:
                    res_decoded=['' for i in range(len(result[1]))] #если запрос нулевой, то мы возвращаем пустую строку и названия столбцов
                    self.pushButton4.setDisabled(True) #кнопка также неактивна, так как запрос пустой
            table_model = MyTableModel(res_decoded, result[1],self) #формируем модель для просмотра таблицы
        except:
            table_model = MyTableModel([['Verify the query']],['Error'],self)
        finally:
            self.tableView.setModel(table_model) #устанавливаем модель
        try:
            RP.close() #закрываем соединение
        except psycopg2.DatabaseError, e:
            print e

    def upload_parser(self,text):
        "запрос по загрузки статьи можно начать оформлять также при помощи запроса, окно откроется автоматически"
        if ' en' in text: #запрос состоит из двух частей: название файла со статьей и язык
            self.text = [text.replace(' en',''),'en']
        elif ' ru' in text: #формируем text=[name, lang]
            self.text = [text.replace(' ru',''),'ru']
        self.openDialog() #открывается диалоговая форма
    def openDialog(self):
        self.openNewWindow = DialogForm(self, text=self.text, app=self.app) #открытие диалоговой формы, где text = [name, lang]
        self.openNewWindow.show() #показать окно
    def openGraph(self):
        self.openGraphPrinter.show() #функция ужке вызвана в _init_, здесь происходит только открытие окна
    def openHelp(self):
        self.openHelpWindow = HelpForm(self, app=self.app, lan=self.lang) #вызов конструктора диалогового окна по построению графа
        self.openHelpWindow.show() #функция ужке вызвана в _init_, здесь происходит только открытие окна

class MyTableModel(QtCore.QAbstractTableModel):
    """
    создание модели для отображения таблицы
    """
    def __init__(self, datain, colnames, parent=None, *args):
        QtCore.QAbstractTableModel.__init__(self, parent, *args)
        self.arraydata = datain #строки результата
        self.colLabels = colnames #имена столбцов

    def rowCount(self, parent):
        return len(self.arraydata) #подсчет количества строк

    def columnCount(self, parent):
        return len(self.arraydata[0]) #подсчет количества столбцов

    def data(self, index, role):
        if not index.isValid():
            return self.QVariant()
        elif role != QtCore.Qt.DisplayRole:
            return QtCore.QVariant()
        return QtCore.QVariant(self.arraydata[index.row()][index.column()])

    def headerData(self, section, orientation, role):
        if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
            return QtCore.QVariant(self.colLabels[section])
        return QtCore.QVariant()

class DialogForm(QtGui.QDialog):
    """
    конструктор диалогового окна по загрузке статьи
    """
    def __init__(self, parent = None, text = None, app=None):
        super(DialogForm, self).__init__(parent)
        self.app=app # QtApplication для смены языка
        # динамически загружает визуальное представление формы
        uic.loadUi("dialog.ui", self)
        self.setWindowTitle(self.app.translate('Dialog','Upload Window')) #Изменение названия окна
        self.lineEdit1.setText(text[0]) #добавление имени в строку (если было определено, если нет, то = '')
        self.lineEdit5.setText(text[1]) #добавление языка в строку (если было определено, если нет, то = '')
        self.buttonBox.accepted.connect(self.upload_info) #если выбрать ОК, то произойдет автоматическая загрузка статьи
        self.lineEdit2.setPlaceholderText('http://') #подсказка для ввода ресурса
        self.lineEdit3.setInputMask('00-00-0000') #маска даты
        self.lineEdit5.setPlaceholderText('en/ru') #подсказка для выбора языка

    def upload_info(self):
        """
        :return: загрузка статьи и обновление БД
        """
        try:
            textname = unicode(self.lineEdit1.text()) #название статьи
            source = str(self.lineEdit2.text()) #ресурс
            date = str(self.lineEdit3.text()).split('-') #дата
            date = datetime.date(int(date[2]),int(date[1]),int(date[0])) #перевод в формат даты Python, необходимый для psycopg2
            language = str(self.lineEdit5.text()) #язык
            if language!='en' and language!='ru':
                raise Exception()
            try:
                UP = request_parser.UploadParser() #вызов парсера для формирования SQL-запроса
                UP.connect() #установка соединения
                self.names = UP.return_IEnames([textname,source, date,language]) #загрузка статьи
                if self.checkBox.isChecked() == True: #если выбран ручной анализ
                    self.openUpload('\n'.join(self.names)) #обработать имена в окне
                    self.names = self.openNewForm.newText #сохранить новые имена
                self.clusters = UP.return_clusters(self.names) #получить кластеры
                if self.checkBox.isChecked() == True: #если выбран ручной анализ
                    self.openUpload(self.clusters) #обработать кластеры в окне
                    self.clust_parser(self.openNewForm.newText) #сохранить новые кластеры
                self.copies = UP.verify_clusters(self.clusters) #проверка кластеров, существуют ли они уже в списке кластеров
                if self.copies!=[]: #если совпадения-таки обнаружены
                    copy=''
                    for name in self.copies: #добавляем имена в строку
                        for var in name:
                            for val in var:
                                copy+=str(val)+', '
                            copy+='\n'
                    self.openUpload(copy.replace(', \n','\n'))
                    copies = self.clust_replacement(self.openNewForm.newText) #обработать имеющиеся кластеры
                self.answer = UP.return_result(self.clusters, copies) #вернуть кластеры для записи, а также копии, которые необходимо использовать
                try: #закрытие соединения
                    UP.close()
                except psycopg2.DatabaseError, e:
                    print e
            except:
                self.answer = u'en: Error while loading\nru: Ошибка загрузки\nErreur de chargement'
            finally:
                self.openAnswer(self.answer)
        except Exception:
            self.answer = u'en: Error while loading\nru: Ошибка загрузки\nErreur de chargement'
            self.openAnswer(self.answer)

    def openUpload(self, text):
        """
        окно для обработки результатов
        :param text: текст для вывода на экран
        :return: -
        """
        self.openNewForm = UploadForm(self, text, app=self.app)
        self.openNewForm.exec_()

    def openAnswer(self,text):
        """
        отчет по загрузке статьи
        :return: окно с результатом загрузки
        """
        self.openNewAnswer = AnswerForm(self, text, app=self.app)
        self.openNewAnswer.exec_()

    def clust_parser(self, clust_list):
        """
        разделить список кластеров на отдельные кластеры
        clust_list: список кластеров
        """
        self.clusters = {}
        for i in clust_list:
            if i != u'':
                splitter = i.split(' :: ')
                self.clusters[splitter[0]] = splitter[1].split(', ')

    def clust_replacement(self, clust_list):
        """
        создает словарь кластеров для замены
        """
        dic_copies = {}
        del clust_list[len(clust_list)-1] #удаляем u''
        for clust in clust_list:
            a = clust.split(', ') # делим по ', '
            dic_copies[a[1]] = int(a[0]) #добавляем в словарь кластер : id
        return dic_copies

class AnswerForm(QtGui.QDialog):
    """
    окно отчета о загрузке статьи
    """
    def __init__(self, parent = None, text = None, app=None):
        super(AnswerForm, self).__init__(parent)
        self.app=app #QApplication для смены языка
        # динамически загружает визуальное представление формы
        uic.loadUi("answer.ui", self)
        self.setModal(True)
        self.setWindowTitle(self.app.translate('Answer','Answer Window')) #Изменение названия окна
        self.textBrowser.setText(text) #текст о результате загрузки
        self.pushButton.clicked.connect(self.close) #закрытие окна при нажатии ОК

class UploadForm(QtGui.QDialog):
    """
    окно отчета о загрузке этапа
    """
    def __init__(self, parent = None, text = None, app=None):
        super(UploadForm, self).__init__(parent)
        if type(text) == dict:
            self.text = self.update(text)
        else:
            self.text = text
        self.state = False
        self.app=app #QApplication для смены языка
        # динамически загружает визуальное представление формы
        uic.loadUi("load.ui", self)
        self.setModal(True)
        self.setWindowTitle(self.app.translate('Loading','Loading Window')) #Изменение названия окна
        self.textEdit.setText(self.text) #текст о результате загрузки
        self.pushButton.clicked.connect(self.closing) #закрытие окна при нажатии ОК

    def closing(self):
        """
        при закрытии сохраняем текст и делим построчно
        """
        self.newText = unicode(self.textEdit.toPlainText())
        self.newText = self.newText.split('\n')
        self.close()

    def update(self, text):
        """
        если на вход передали словарь, то ддля его отображения на экране в форме: слово :: варианты слова в тексте,
        необходимо произвести обработку вывода
        """
        new_text = ''
        for i in text:
            new_text+=i+' :: '+', '.join(text[i])+'\n'
        return new_text


class GraphForm(QtGui.QDialog):
    """
    конструктор окна с графом
    """
    def __init__(self, parent = None, req=None, clusttexts=None, app=None):
        super(GraphForm, self).__init__(parent)
        self.app = app #QApplication для смены языка
        self.req=req #текст запроса = название файлов
        self.text = ''
        self.clusttexts = clusttexts #словарь с именами и статьями для каждго имени
        # динамически загружает визуальное представление формы
        uic.loadUi("graph.ui", self)
        self.setWindowTitle(self.app.translate('Graph','Graph Window')) #Изменение названия окна
        self.sc = MyStaticMplCanvas(self.widget, width=5, height=4, dpi=100, req=req, clusttexts=self.clusttexts) #отображение графа в QWidget
        self.verticalLayout.addWidget(self.sc) #добавление виджета sc - основа графа
        self.widget.setFocus() #элемент компоновки
        self.pushButton.clicked.connect(self.setBrowserText) #отображение свойств графа при нажатии клавиш
        self.pushButton_5.clicked.connect(self.saveGraph) #сохранить граф в папку Graphs
        self.pushButton_7.clicked.connect(self.saveAll) #сохранить граф в папку Graphs, а информацию по графу в папку Info

    def setBrowserText(self):
        """
        :return: свойства графа
        """
        text = self.sc.graph_parameters() #возвращает свойства графа
        self.textBrowser.setText(text)

    def saveGraph(self):
        """
        :return: изобрание в папке Graphs
        """
        self.sc.build_graph()

    def saveAll(self):
        """
        сохранить граф в виде изображение и текстовое описание свойств графа
        :return: изобрание графа в папке Graphs, текстовый файл в папке Graphs/Info
        """
        self.sc.saveText() #вызов функции по сохранению информации о графе
        if os.path.exists('Graphs/'+self.req+".png"): #если файл еще не создан, то сохранить изображение
            pass
        else:
            self.saveGraph()


class HelpForm(QtGui.QDialog):
    """
    конструктор окна помощи
    """
    def __init__(self, parent = None,app=None, lan='en'):
        super(HelpForm, self).__init__(parent)
        self.app = app #QApplication для смены язык
        # динамически загружает визуальное представление формы
        uic.loadUi("help.ui", self)
        self.setWindowTitle(self.app.translate('Help','Help Window')) #установка заголовка окна
        text_help = codecs.open('manual_'+lan+'.html','r', 'utf-8').read()
        self.textBrowser.setText(text_help) #здесь располагается текст справки

class MyMplCanvas(FigureCanvas):
    """Основа для отображения графа"""
    def __init__(self, parent=None, width=5, height=4, dpi=100, req=None, clusttexts=None):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = fig.add_subplot(111)
        # We want the axes cleared every time plot() is called
        self.axes.hold(False)
        self.req = req #текст запроса (для имени изображения
        self.clusttexts = clusttexts #кластеры для построения графа (подлежат обработке)
        self.compute_initial_figure()
        FigureCanvas.__init__(self, fig)
        self.setParent(parent)
        FigureCanvas.setSizePolicy(self,
                                   QtGui.QSizePolicy.Expanding,
                                   QtGui.QSizePolicy.Expanding)
        FigureCanvas.updateGeometry(self)

    def compute_initial_figure(self):
        pass

class MyStaticMplCanvas(MyMplCanvas):
    """Simple canvas with a sine plot."""
    def compute_initial_figure(self):
        G=nx.Graph() #вызов графа
        dic = self.buildEdges(self.clusttexts) #преобразование в форму [text, [name1,name2,name3]]
        for i in dic: #для каждого текста
            for j in i[1]: #для каждого героя в тексте
                for k in i[1]: #с каждым героем в тексте
                    G.add_edge(j, k) #строится ребро
        bc=nx.betweenness_centrality(G)
        self.bc = [(bc[i]+0.005) * 50000 for i in G.nodes()]  #betweenness centrality для каждой вершины
        cc=nx.closeness_centrality(G)
        self.cc=[cc[i] for i in G.nodes()] #closeness_centrality для каждой вершины
        self.degree = nx.degree(G)
        try:
            self.diameter = nx.diameter(G)
        except:
            pass
        self.clustering = nx.average_clustering(G)
        self.density = float(len(nx.edges(G)))/((len(nx.nodes(G))-1)*len(nx.nodes(G))/2)
        self.G = G
        nx.draw(G,node_size=self.bc, node_color=self.cc, alpha=0.5,font_family='verdana', ax=self.axes)

    def buildEdges(self, clusttexts):
        """
        :param clusttexts: словарь, героев и список их текстов
        :return: список текстов и героев в каждом из них
        """
        texts = list(set([el for lst in clusttexts.values() for el in lst])) #создаем список текстов
        allTexts = [] #пустой список для хранения в формате [text, [pers1,pers2,pers3]]
        for text in texts:
            l = [] #список персонажей
            for cluster in clusttexts: #для каждого персонажа
                if text in clusttexts.get(cluster): #если он упоминается в тексте
                    l.append(cluster.decode('utf-8')) #персонаж вносится в список
            allTexts.append([text,l]) #сохранение в формате [text, [pers1,pers2,pers3]]
        return allTexts

    def graph_parameters(self):
        """
        :return: текст с парамтерами графа
        """
        text = 'query: '+self.req+'\n'
        text+="betweenness centrality = "+str(self.bc)+'\n'
        text+="closeness centrality = "+str(self.cc)+'\n'
        try:
            text+="diameter = "+str(self.diameter)+'\n'
        except:
            pass
        text+="average clustering = "+str(self.clustering)+'\n'
        text+="degree = "+str(self.degree)+'\n'
        text+="density = "+str(self.density)
        return text

    def build_graph(self):
        nx.draw(self.G,node_size=self.bc, node_color=self.cc, alpha=0.5, font_family='verdana') #построение графа
        plt.savefig('Graphs/'+self.req+'.png') #сохранение графа в папку Graphs

    def saveText(self):
        """
        :return: txt файл с параметрами графа
        """
        text = self.graph_parameters()
        with codecs.open('Graphs/Info/'+self.req+'.txt', 'w', encoding='utf-8') as wr:
            wr.write(text)



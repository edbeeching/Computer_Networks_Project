import sys
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
import qdarkstyle


class UserInterace(QMainWindow):
    def __init__(self):
        super().__init__()

        self.initUI()


    def initUI(self):
        pass
        # set title etc
        # Splitter view
        # status bar
        # tool bar
        # file menu

        # set title etc
        self.setGeometry(100, 100, 800, 600)
        self.setWindowTitle('torrentNchill')
        self.main_widget = QWidget()
        self.setCentralWidget(self.main_widget)

        # file menu
        self.init_menu()
        # tool bar

        # Splitter view
        vbox = self.init_splitter()
        self.main_widget.setLayout(vbox)
        # self.setLayout(vbox)






        self.show()

    def init_menu(self):
        # Actions: Load file, exit
        load_action = QAction(QIcon('icons/load.png'), '&Load Orch', self)
        load_action.setShortcut('Ctrl+L')
        load_action.setStatusTip('Load Orchestra file')
        load_action.triggered.connect(self.load_orch)
        exit_action = QAction(QIcon('exit.png'), '&Exit', self)
        exit_action.setShortcut('Ctrl+Q')
        exit_action.setStatusTip('Exit application')
        exit_action.triggered.connect(qApp.quit)

        test_action = QAction(QIcon('icons/test.png'), '&Test', self)
        test_action.setShortcut('Ctrl+T')
        test_action.setStatusTip('Test action')
        test_action.triggered.connect(self.test)





        menu_bar = self.menuBar()
        file_menu = menu_bar.addMenu('&File')
        file_menu.addAction(load_action)
        file_menu.addAction(exit_action)

        self.toolbar = self.addToolBar('Toolbar')
        self.toolbar.addAction(load_action)
        self.toolbar.addAction(test_action)

    def test(self):
        print("adding row")
        self.model.appendRow([QStandardItem('maxresdefault.jpg'),     QStandardItem('142044'),     QStandardItem('0 % '),
                                            QStandardItem('Downloading'),   QStandardItem('10'),  QStandardItem('128 KB/s'),
                                            QStandardItem('56 KB/s')])


    def orch_file_dialog(self):
        print("orch file dialog")
        file_name = QFileDialog.getOpenFileName(self, 'Open file')
        return file_name

    def load_orch(self):
        print('Loading orch file')
        file = self.orch_file_dialog()

        print(file)




    def init_splitter(self):

        self.tree_widget = QTreeView()
        self.tree_widget.setEditTriggers(QAbstractItemView.NoEditTriggers)
        #self.tree_widget.editTriggers(QAbstract)
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels([
                                                'File',     'Size',     ' % ',
                                                'Status',   'Members',  'Down Speed',
                                                'Up Speed'
                                            ])

        self.model.appendRow([QStandardItem('File'),     QStandardItem('Size'),     QStandardItem(' % '),
                              QStandardItem('Status'),   QStandardItem('Members'),  QStandardItem('Down Speed'),
                              QStandardItem('Up Speed')])

        self.model.appendRow([QStandardItem('File'),     QStandardItem('Size'),     QStandardItem(' % '),
                              QStandardItem('Status'),   QStandardItem('Members'),  QStandardItem('Down Speed'),
                              QStandardItem('Up Speed')])
        self.tree_widget.setModel(self.model)
        self.tree_widget.setUniformRowHeights(True)

        vbox = QVBoxLayout(self)
        top = QFrame(self)
        #top.te

        top.setFrameShape(QFrame.StyledPanel)

        tbox = QHBoxLayout(self)
        tbox.addWidget(self.tree_widget)
        top.setLayout(tbox)

        bot = QFrame(self)
        bot.setFrameShape(QFrame.StyledPanel)
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(top)
        splitter.addWidget(bot)
        vbox.addWidget(splitter)
        return vbox

    def on_click(self):
        print("clicked")
        print(self.table_widget.selectedRanges())
        for item in self.table_widget.selectedItems():
            print(item.row(), item.column(), item.text())

if __name__ == "__main__":

    app = QApplication(sys.argv)
    # app.setStyleSheet(qdarkstyle.load_stylesheet_pyqt5())
    gui = UserInterace()
    sys.exit(app.exec_())
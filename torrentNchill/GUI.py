#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

Creating a skeleton of a basic GUI.
This is to only test if the libraries work.
Difficulty: using PyQt5 on Mac.

To do: Add Pause, Stop, Start feature.

Created on Mon Feb 6
@author: Sejal
Last updated: Tue Feb 14

"""

import sys
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *


class GUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        pass
        self.setGeometry(100, 100, 800, 600)
        self.setWindowTitle('torrentNchill')
        self.main_widget = QWidget()
        self.setCentralWidget(self.main_widget)
        self.init_menu()
        #Left Splitter view
        #lbox = self.init_left_splitter()
        #self.main_widget.setLayout(lbox)
        # Splitter view
        rbox = self.init_right_splitter()
        self.main_widget.setLayout(rbox)
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

    def right_click(self, pos):
        menu = QMenu()
        Action = menu.addAction("Remove")
        action = menu.exec_(self.mapToGlobal(pos))
        #action = menu.exec_(self.globalPos())
        if action == Action:
            print('hello')
            #qApp.quit()

    def test(self):
        print("adding row")
        self.model.appendRow([
            QStandardItem('maxresdefault.jpg'), QStandardItem('142044'), QStandardItem('0 % '),
            QStandardItem('Downloading'), QStandardItem('10'), QStandardItem('128 KB/s'),QStandardItem('56 KB/s')
        ])
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.right_click)

    def orch_file_dialog(self):
        print("orch file dialog")
        file_name = QFileDialog.getOpenFileName(self, 'Open file')
        return file_name

    def load_orch(self):
        print('Loading orch file')
        file = self.orch_file_dialog()
        print(file)

    #Add this later if the side panels are needed.
    def init_left_splitter(self):
        hbox = QHBoxLayout(self)

        topleft = QFrame()
        topleft.setFrameShape(QFrame.StyledPanel)
        bottom = QFrame()
        bottom.setFrameShape(QFrame.StyledPanel)

        splitter1 = QSplitter(Qt.Horizontal)
        textedit = QTextEdit()
        splitter1.addWidget(topleft)
        splitter1.addWidget(textedit)
        splitter1.setSizes([100, 200])

        splitter2 = QSplitter(Qt.Vertical)
        splitter2.addWidget(splitter1)
        splitter2.addWidget(bottom)

        hbox.addWidget(splitter2)

        self.setLayout(hbox)
        QApplication.setStyle(QStyleFactory.create('Cleanlooks'))

        self.setGeometry(300, 300, 300, 200)
        self.setWindowTitle('QSplitter demo')
        self.show()

    def init_right_splitter(self):
        self.tree_widget = QTreeView()
        self.tree_widget.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels([
            'File', 'Size', ' % ','Status', 'Members', 'Down Speed','Up Speed'
        ])

        self.tree_widget.setModel(self.model)
        self.tree_widget.setUniformRowHeights(True)

        vbox = QVBoxLayout(self)
        top = QFrame(self)

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
        print('hello')
        return vbox

if __name__ == "__main__":
    app = QApplication(sys.argv)
    interface = GUI()
    sys.exit(app.exec_())
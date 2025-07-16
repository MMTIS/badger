# views.py
from PySide6.QtCore import Qt, Signal, QModelIndex, QEvent
from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QSplitter, QComboBox, QListView, QTabWidget, QLineEdit, QPushButton, QLabel
from gui.models import ReferenceListModel
from netexio.database import Tid


class CtrlClickListView(QListView):
    ctrlClicked = Signal(QModelIndex)

    def mousePressEvent(self, event: QEvent):
        if event.modifiers() & Qt.KeyboardModifier.ControlModifier:
            index = self.indexAt(event.pos())
            if index.isValid():
                self.ctrlClicked.emit(index)
                return
        super().mousePressEvent(event)


class PerspectiveWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.splitter = QSplitter()
        main_layout = QHBoxLayout(self)
        main_layout.addWidget(self.splitter)
        main_layout.setContentsMargins(0, 0, 0, 0)
        self.splitter.addWidget(self._create_left_panel())
        self.splitter.addWidget(self._create_right_panel())
        self.splitter.setSizes([350, 850])

    def _create_left_panel(self):
        left_widget = QWidget()
        layout = QVBoxLayout(left_widget)
        self.db_combo_box = QComboBox()
        filter_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Filter objecten...")
        self.clear_filter_button = QPushButton("X")
        self.clear_filter_button.setFixedWidth(30)
        filter_layout.addWidget(self.search_input)
        filter_layout.addWidget(self.clear_filter_button)
        self.list_view_left = CtrlClickListView()
        self.list_view_left.setAlternatingRowColors(True)
        layout.addWidget(QLabel("Database:"))
        layout.addWidget(self.db_combo_box)
        layout.addWidget(QLabel("Filter:"))
        layout.addLayout(filter_layout)
        layout.addWidget(self.list_view_left)
        return left_widget

    def _create_right_panel(self):
        right_widget = QWidget()
        layout = QVBoxLayout(right_widget)
        self.details_tab_widget = QTabWidget()
        ref_widget = QWidget()
        ref_layout = QVBoxLayout(ref_widget)
        ref_layout.setContentsMargins(0, 0, 0, 0)
        ref_splitter = QSplitter(Qt.Horizontal)
        self.list_view_incoming = CtrlClickListView()
        self.list_view_outgoing = CtrlClickListView()
        self.incoming_ref_model = ReferenceListModel()
        self.outgoing_ref_model = ReferenceListModel()
        self.list_view_incoming.setModel(self.incoming_ref_model)
        self.list_view_outgoing.setModel(self.outgoing_ref_model)
        incoming_widget = QWidget()
        incoming_layout = QVBoxLayout(incoming_widget)
        incoming_layout.addWidget(QLabel("Incoming References:"))
        incoming_layout.addWidget(self.list_view_incoming)
        outgoing_widget = QWidget()
        outgoing_layout = QVBoxLayout(outgoing_widget)
        outgoing_layout.addWidget(QLabel("Outgoing References:"))
        outgoing_layout.addWidget(self.list_view_outgoing)
        ref_splitter.addWidget(incoming_widget)
        ref_splitter.addWidget(outgoing_widget)
        ref_layout.addWidget(ref_splitter)
        layout.addWidget(QLabel("Details:"))
        layout.addWidget(self.details_tab_widget, stretch=2)
        layout.addWidget(ref_widget, stretch=1)
        return right_widget

    def set_list_model(self, model):
        self.list_view_left.setModel(model)

    def update_reference_lists(self, incoming: tuple[tuple[type[Tid], str, str], bool], outgoing: tuple[tuple[type[Tid], str, str], bool]):
        self.incoming_ref_model.populate(incoming)
        self.outgoing_ref_model.populate(outgoing)


class MainView(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("LMDB Explorer")
        self.setGeometry(100, 100, 1200, 800)
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.setCentralWidget(self.tab_widget)
        self._create_menu_bar()

    def _create_menu_bar(self):
        menu_bar = self.menuBar()
        file_menu = menu_bar.addMenu("&Bestand")
        self.add_tab_action = file_menu.addAction("Nieuw Perspectief")
        file_menu.addSeparator()
        self.exit_action = file_menu.addAction("Afsluiten")

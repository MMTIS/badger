from PySide6.QtWidgets import QTabWidget, QMainWindow, QWidget


class MainView(QMainWindow):
    def __init__(self, parent: QWidget | None):
        super().__init__()
        self.setWindowTitle("NeTEx Explorer")
        self.setGeometry(100, 100, 1200, 800)
        self.tab_widget = QTabWidget(parent)
        self.tab_widget.setTabsClosable(True)
        self.setCentralWidget(self.tab_widget)
        self._create_menu_bar()

    def _create_menu_bar(self) -> None:
        menu_bar = self.menuBar()
        file_menu = menu_bar.addMenu("&Bestand")
        self.add_tab_action = file_menu.addAction("Nieuw Perspectief")
        file_menu.addSeparator()
        self.exit_action = file_menu.addAction("Afsluiten")

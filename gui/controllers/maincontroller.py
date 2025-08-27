from typing import Optional
from PySide6.QtCore import QObject

from gui.controllers.perspectivecontroller import PerspectiveController
from gui.controllers.storagecontroller import StorageController
from gui.models.storageobject import StorageObject
from gui.views.mainwindow import MainView
from storage.interface import Storage


class MainController(QObject):
    def __init__(self, app, storage: Storage):
        super().__init__()
        self.app = app
        self.database = storage
        self.view = MainView(None)
        self.perspective_controllers: list[QObject] = []
        self.storage_controller = StorageController(storage)

        self.view.exit_action.triggered.connect(self.app.quit)
        self.view.add_tab_action.triggered.connect(self.add_new_perspective)
        self.view.tab_widget.tabCloseRequested.connect(self.close_perspective)

        self.add_new_perspective()

    def show(self) -> None:
        self.view.show()

    def add_new_perspective(self, lmdbo_to_show: Optional[StorageObject] = None) -> None:
        from gui.widgets.perspective import PerspectiveWidget  # Late import om circulaire afhankelijkheid te vermijden

        p_widget = PerspectiveWidget(self.view)
        p_controller = PerspectiveController(self.storage_controller, p_widget)
        p_controller.request_new_perspective.connect(self.add_new_perspective)
        p_controller.titleChanged.connect(self.on_perspective_title_changed)
        self.perspective_controllers.append(p_controller)
        default_title = p_widget.db_combo_box.currentText()
        tab_title = getattr(lmdbo_to_show.obj, 'id', default_title) if lmdbo_to_show else default_title
        _tab_index = self.view.tab_widget.addTab(p_widget, tab_title)
        if lmdbo_to_show:
            p_controller.set_initial_state(lmdbo_to_show)
        else:
            first_clazz = p_controller.widget.db_combo_box.itemData(0)
            if first_clazz:
                p_controller.set_initial_state_by_class(first_clazz)
        self.view.tab_widget.setTabsClosable(self.view.tab_widget.count() > 1)

    def on_perspective_title_changed(self, new_title: str) -> None:
        sender_controller = self.sender()
        if not sender_controller:
            return
        for i in range(self.view.tab_widget.count()):
            controller = next((c for c in self.perspective_controllers if c.widget is self.view.tab_widget.widget(i)), None)
            if controller is sender_controller:
                self.view.tab_widget.setTabText(i, new_title)
                break

    def close_perspective(self, index) -> None:
        if self.view.tab_widget.count() <= 1:
            return
        widget = self.view.tab_widget.widget(index)
        self.view.tab_widget.removeTab(index)
        controller = next((c for c in self.perspective_controllers if c.widget is widget), None)
        if controller:
            self.perspective_controllers.remove(controller)
            controller.deleteLater()
        self.view.tab_widget.setTabsClosable(self.view.tab_widget.count() > 1)

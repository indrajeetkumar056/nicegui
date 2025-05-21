from nicegui import ui, app
import os
from app.niceGUI.manual_card_backfill import open_manual_card
from app.niceGUI.utils.logger_ui import LOGGER_REGISTRY, LoggerUI

app.add_static_files('/static', 'app/niceGUI/static')  # Serve static files
app.add_static_files('/docs', 'docs')  # Serve documentation files

def run_nicegui():
    print("Running in NiceGUI mode...")

    # Include CSS file from static folder
    ui.add_head_html('<link rel="stylesheet" type="text/css" href="/static/styles.css">')

    # Title row with documentation icon
    with ui.row().classes('quanthub-header'):
        ui.label('QH-BACKFILL').classes('quanthub-title')

        # Docs Expansion Panel instead of new tab
        with ui.expansion("Documentation", icon="description").classes('docs-help'):
            ui.link("📄 Sources Documentation", "/docs/sources/index.html", new_tab=True).classes('docs-link')
            ui.link("📄 Sinks Documentation", "/docs/sinks/index.html", new_tab=True).classes('docs-link')

    # Main UI
    with ui.row().classes('main-container'):
        with ui.column().classes('content-container'):  # 80% width
            with ui.tabs().classes('nicegui-tabs') as tabs:
                manual_tab = ui.tab('Manual').classes('q-tab')

            with ui.tab_panels(tabs).classes('w-full nicegui-tab-panels'):
                with ui.tab_panel(manual_tab):
                    with ui.row().classes('manual-tab-container'):
                        with ui.column().classes('manual-card-container'):
                            open_manual_card()
                        
                        with ui.column().classes('logger-container'):
                            ui.label("Logs").classes('text-2xl font-bold text-center w-full mt-4')
                            LOGGER_REGISTRY["manual_logger"] = LoggerUI(ui.column(), name="manual_logger")

    ui.run(port=2000, title='Backfilling Page', storage_secret=None) 

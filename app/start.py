import argparse

from app.terminal.main import run_terminal
from app.niceGUI.main import run_nicegui

if __name__ in {"__main__", "__mp_main__"}:
    parser = argparse.ArgumentParser(description="Run modules in Terminal or NiceGUI mode")
    parser.add_argument("--terminal", action="store_true", help="Mode to run on terminal")
    parser.add_argument("--nicegui", action="store_true", help="NiceGUI mode")

    args = parser.parse_args()

    if args.terminal:
        run_terminal()
    elif args.nicegui:
        run_nicegui()
    else:
        parser.print_help()

        

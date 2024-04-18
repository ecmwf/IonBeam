from ionbeam.core.bases import Action
from ionbeam.core.config_parser import Subclasses

def test_actions():
    # Get all the subclasses of action
    actions = Subclasses().get(Action)


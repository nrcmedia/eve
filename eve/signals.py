from blinker import Namespace

signalizer = Namespace()
pre_insert = signalizer.signal('pre-insert')
pre_update = signalizer.signal('pre-update')

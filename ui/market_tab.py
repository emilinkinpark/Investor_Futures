import tkinter as tk
from api.exchange import get_symbols

class MarketTab(tk.Frame):
    def __init__(self, master, on_symbol_select=None):
        super().__init__(master)
        self.on_symbol_select = on_symbol_select
        self.symbols = []
        self.filtered_symbols = []
        self.create_widgets()
        self.load_symbols()
        
    def create_widgets(self):
        # Search frame
        search_frame = tk.Frame(self)
        search_frame.pack(fill=tk.X, padx=5, pady=5)
        
        tk.Label(search_frame, text="Search:").pack(side=tk.LEFT)
        self.search_var = tk.StringVar()
        self.search_var.trace("w", self.filter_symbols)
        search_entry = tk.Entry(search_frame, textvariable=self.search_var)
        search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        # Symbols list
        self.symbol_list = tk.Listbox(self)
        self.symbol_list.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.symbol_list.bind('<<ListboxSelect>>', self.on_list_select)
        
        # Refresh button
        refresh_btn = tk.Button(self, text="Refresh Symbols", command=self.load_symbols)
        refresh_btn.pack(pady=5)
        
    def load_symbols(self):
        self.symbols = get_symbols()
        self.filter_symbols()
            
    def filter_symbols(self, *args):
        search_term = self.search_var.get().upper()
        self.symbol_list.delete(0, tk.END)
        
        if search_term:
            self.filtered_symbols = [s for s in self.symbols if search_term in s]
        else:
            self.filtered_symbols = self.symbols.copy()
            
        for sym in self.filtered_symbols:
            self.symbol_list.insert(tk.END, sym)
            
    def on_list_select(self, event):
        if self.on_symbol_select:
            selection = self.symbol_list.curselection()
            if selection:
                symbol = self.filtered_symbols[selection[0]]
                self.on_symbol_select(symbol)
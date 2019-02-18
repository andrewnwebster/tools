import tkinter
from tkinter import *

def lesson1():
	widget=Label(None, text=tempText)
	widget.pack()
	widget=Label(None, text=tempText)
	widget.pack()
	widget.mainloop()

def lesson2():
	widget=Label(None, text=tempText)
	widget.pack()
	widget.mainloop()

def lesson3():
	Label(text='Hello GUI world!').pack(expand=YES, fill=BOTH)
	mainloop()

def main():
	global tempText
	tempText='Hello!'
	lesson3()

main()
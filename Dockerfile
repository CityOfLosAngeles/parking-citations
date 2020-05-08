FROM irose/citywide-jupyterlab:8896e05986fc

RUN pip install civis 

CMD ["python", "src/update-citations.py"]
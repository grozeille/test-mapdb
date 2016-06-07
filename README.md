Start the Spring Boot project

Open Google Chrome with url http://localhost:8080
Open developer console
Write the Javascript:

```javascript
var source = new EventSource('/stream?requestId=1');
source.addEventListener('message', function(e) {
  var data = JSON.parse(e.data);
  console.log(data.msgId);
  document.body.innerHTML += e.data + "<br/>";
}, false);
```

Try to restart the Web Server to simulate network failures

import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'MacroBase';

  openWizard(): void {
      // document.getElementById("main").style.marginLeft = "20%";
      // document.getElementById("wizard").style.width = "20%";
      document.getElementById("wizard").style.display = "inline-block";
      document.getElementById("openWizard").style.display = 'none';
  }

   closeWizard(): void {
      document.getElementById("main").style.marginLeft = "0%";
      document.getElementById("wizard").style.display = "none";
      document.getElementById("openWizard").style.display = "block";
  }
}
